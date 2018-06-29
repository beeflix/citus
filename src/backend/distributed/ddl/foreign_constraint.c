/*-------------------------------------------------------------------------
 *
 * foreign_constraint.c
 *
 * This file contains functions to create, alter and drop foreign
 * constraints on distributed tables.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_constraint_fn.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "distributed/colocation_utils.h"
#include "distributed/foreign_constraint.h"
#include "distributed/listutils.h"
#include "distributed/master_protocol.h"
#include "distributed/multi_join_order.h"
#include "distributed/version_compat.h"
#include "nodes/pg_list.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "libpq/libpq-be.h"

/*
 * FRelGraph holds the graph data structure for foreign key relation between
 * relations. We will only have single static instance of that struct and it
 * will be invalidated after change on any foreign constraint.
 */
typedef struct FRelGraph
{
	HTAB *nodeMap;
	bool isValid;
}FRelGraph;

/*
 * FRelNode holds the data for each node of the FRelGraph. For each node we have
 * relation id, which is the Oid of that relation, visiting information for that
 * that node in the latest DFS and the list of adjacency nodes. Note that we also
 * hold back adjacency nodes for getting referenced node over that one.
 */
typedef struct FRelNode
{
	Oid relationId;
	bool visited;
	List *adjacencyList;
	List *backAdjacencyList;
}FRelNode;


/*
 * FRelEdge will only be used while creating the FRelGraph. It won't show edge
 * information on the graph, yet will be used in the pre-processing phase.
 */
typedef struct FRelEdge
{
	Oid referencingRelationOID;
	Oid referencedRelationOID;
}FRelEdge;


static FRelGraph *frelGraph = NULL;

static void CreateForeignKeyRelationGraph(void);
static void PopulateAdjacencyLists(void);
static int CompareFRelEdges(const void *leftElement, const void *rightElement);
static void AddEdge(HTAB *adjacencyLists, Oid referencingOid, Oid referencedOid);
static FRelNode * CreateOrFindNode(HTAB *adjacencyLists, Oid relid);
static void GetConnectedListHelper(FRelNode *node, List **adjacentNodeList, bool
								   isReferencing);
static List * GetForeignConstraintRelationsHelper(Oid relationId, bool isReferencing);

/* this function is only exported in the regression tests */
PG_FUNCTION_INFO_V1(get_referencing_relation_id_list);
PG_FUNCTION_INFO_V1(get_referenced_relation_id_list);

/*
 * get_referencing_relation_id_list returns the list of table oids that is referencing
 * by given oid recursively. It uses the foreign key relation graph if it exists
 * in the cache, otherwise forms it up once.
 */
Datum
get_referencing_relation_id_list(PG_FUNCTION_ARGS)
{
	FuncCallContext *functionContext = NULL;
	ListCell *foreignRelationCell = NULL;

	CheckCitusVersion(ERROR);

	/* for the first we call this UDF, we need to populate the result to return set */
	if (SRF_IS_FIRSTCALL())
	{
		Oid relationId = PG_GETARG_OID(0);
		DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(relationId);
		List *refList = cacheEntry->referencingRelationsViaForeignKey;

		/* create a function context for cross-call persistence */
		functionContext = SRF_FIRSTCALL_INIT();

		foreignRelationCell = list_head(refList);
		functionContext->user_fctx = foreignRelationCell;
	}

	/*
	 * On every call to this function, we get the current position in the
	 * statement list. We then iterate to the next position in the list and
	 * return the current statement, if we have not yet reached the end of
	 * list.
	 */
	functionContext = SRF_PERCALL_SETUP();

	foreignRelationCell = (ListCell *) functionContext->user_fctx;
	if (foreignRelationCell != NULL)
	{
		Oid refId = lfirst_oid(foreignRelationCell);

		functionContext->user_fctx = lnext(foreignRelationCell);

		SRF_RETURN_NEXT(functionContext, PointerGetDatum(refId));
	}
	else
	{
		SRF_RETURN_DONE(functionContext);
	}
}


/*
 * get_referenced_relation_id_list returns the list of table oids that is referenced
 * by given oid recursively. It uses the foreign key relation graph if it exists
 * in the cache, otherwise forms it up once.
 */
Datum
get_referenced_relation_id_list(PG_FUNCTION_ARGS)
{
	FuncCallContext *functionContext = NULL;
	ListCell *foreignRelationCell = NULL;

	CheckCitusVersion(ERROR);

	/* for the first we call this UDF, we need to populate the result to return set */
	if (SRF_IS_FIRSTCALL())
	{
		Oid relationId = PG_GETARG_OID(0);
		DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(relationId);
		List *refList = cacheEntry->referencedRelationsViaForeignKey;

		/* create a function context for cross-call persistence */
		functionContext = SRF_FIRSTCALL_INIT();

		foreignRelationCell = list_head(refList);
		functionContext->user_fctx = foreignRelationCell;
	}

	/*
	 * On every call to this function, we get the current position in the
	 * statement list. We then iterate to the next position in the list and
	 * return the current statement, if we have not yet reached the end of
	 * list.
	 */
	functionContext = SRF_PERCALL_SETUP();

	foreignRelationCell = (ListCell *) functionContext->user_fctx;
	if (foreignRelationCell != NULL)
	{
		Oid refId = lfirst_oid(foreignRelationCell);

		functionContext->user_fctx = lnext(foreignRelationCell);

		SRF_RETURN_NEXT(functionContext, PointerGetDatum(refId));
	}
	else
	{
		SRF_RETURN_DONE(functionContext);
	}
}


/*
 * IsForeignKeyGraphValid check whether there is a valid graph.
 */
bool
IsForeignKeyGraphValid()
{
	if (frelGraph != NULL && frelGraph->isValid)
	{
		return true;
	}

	return false;
}


/*
 * SetForeignKeyGraphInvalid sets the validity of the graph to false.
 */
void
SetForeignKeyGraphInvalid()
{
	if (frelGraph != NULL)
	{
		frelGraph->isValid = false;
	}
}


/*
 * ReferencedRelationIdList is a wrapper function around GetRefenceRelationIdHelper
 * to get list of relation IDs which are referenced by the given relation id.
 * Note that, if relation A is referenced by relation B and relation B is referenced
 * by relation C, then the result list for relation A consists of the relation
 * IDs of relation B and relation C.
 */
List *
ReferencedRelationIdList(Oid relationId)
{
	return GetForeignConstraintRelationsHelper(relationId, false);
}


/*
 * ReferencingRelationIdList is a wrapper function around GetRefenceRelationIdHelper
 * to get list of relation IDs which are referencing by the given relation id.
 * Note that, if relation A is referenced by relation B and relation B is referenced
 * by relation C, then the result list for relation C consists of the relation
 * IDs of relation A and relation B.
 */
List *
ReferencingRelationIdList(Oid relationId)
{
	return GetForeignConstraintRelationsHelper(relationId, true);
}


/*
 * GetForeignConstraintRelationsHelper returns the list of oids referenced or
 * referencing given relation id. It is a helper function for providing results
 * to public functions ReferencedRelationIdList and ReferencingRelationIdList.
 */
static List *
GetForeignConstraintRelationsHelper(Oid relationId, bool isReferencing)
{
	List *foreignKeyList = NIL;
	bool isFound = false;
	FRelNode *relationNode = NULL;

	CreateForeignKeyRelationGraph();

	relationNode = (FRelNode *) hash_search(frelGraph->nodeMap, &relationId,
											HASH_FIND, &isFound);

	if (!isFound)
	{
		/*
		 * If there is no node with the given relation id, that means given table
		 * is not referencing and does not referenced by any table
		 */
		return NIL;
	}
	else
	{
		List *foreignNodeList = NIL;
		ListCell *nodeCell = NULL;

		GetConnectedListHelper(relationNode, &foreignNodeList, isReferencing);

		/*
		 * We need only their OIDs, we get back node list to make their visited
		 * variable to false for using them iteratively.
		 */
		foreach(nodeCell, foreignNodeList)
		{
			FRelNode *currentNode = (FRelNode *) lfirst(nodeCell);

			foreignKeyList = lappend_oid(foreignKeyList, currentNode->relationId);
			currentNode->visited = false;
		}
	}

	return foreignKeyList;
}


/*
 * GetConnectedListHelper is the function for getting nodes connected (or connecting) to
 * the given relation. adjacentNodeList holds the result for recursive calls and
 * by changing isReferencing caller function can select connected or connecting
 * adjacency list.
 *
 */
static void
GetConnectedListHelper(FRelNode *node, List **adjacentNodeList, bool isReferencing)
{
	ListCell *nodeCell = NULL;
	List *adjacencyList = NIL;

	node->visited = true;
	*adjacentNodeList = lappend(*adjacentNodeList, node);

	if (isReferencing)
	{
		adjacencyList = node->adjacencyList;
	}
	else
	{
		adjacencyList = node->backAdjacencyList;
	}

	foreach(nodeCell, adjacencyList)
	{
		FRelNode *curNode = (FRelNode *) lfirst(nodeCell);
		if (curNode->visited == false)
		{
			GetConnectedListHelper(curNode, adjacentNodeList, isReferencing);
		}
	}
}


/*
 * CreateForeignKeyRelationGraph creates the foreign key relation graph using
 * foreign constraint provided by pg_constraint metadata table.
 */
static void
CreateForeignKeyRelationGraph()
{
	MemoryContext oldContext;
	HASHCTL info;
	uint32 hashFlags = 0;
	MemoryContext foreignRelationMemoryContext = NULL;

	/* if we have already created the graph, use it */
	if (IsForeignKeyGraphValid())
	{
		return;
	}

	ClearForeignKeyRelationGraphContext();

	foreignRelationMemoryContext = AllocSetContextCreateExtended(
		CacheMemoryContext,
		"FRel Graph Context",
		ALLOCSET_DEFAULT_MINSIZE,
		ALLOCSET_DEFAULT_INITSIZE,
		ALLOCSET_DEFAULT_MAXSIZE);

	oldContext = MemoryContextSwitchTo(foreignRelationMemoryContext);

	frelGraph = (FRelGraph *) palloc(sizeof(FRelGraph));
	frelGraph->isValid = false;

	/* create (oid) -> [FRelNode] hash */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(FRelNode);
	info.hash = oid_hash;
	info.hcxt = CurrentMemoryContext;
	hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	frelGraph->nodeMap = hash_create("foreign key relation map (oid)",
									 64 * 32, &info, hashFlags);


	PopulateAdjacencyLists();

	frelGraph->isValid = true;
	MemoryContextSwitchTo(oldContext);
}


/*
 * PopulateAdjacencyLists gets foreign key information from pg_constraint
 * metadata table and populates them to the foreign key relation graph.
 */
static void
PopulateAdjacencyLists(void)
{
	SysScanDesc fkeyScan;
	HeapTuple tuple;
	Relation fkeyRel;

	/*ScanKeyData scanKey[1]; */
	Oid prevReferencingOid = InvalidOid;
	Oid prevReferencedOid = InvalidOid;
	List *frelEdgeList = NIL;
	ListCell *frelEdgeCell = NULL;

	/* we only want foreign keys */
	/*ScanKeyInit(&scanKey[0], Anum_pg_constraint_contypid, */
	/*			BTEqualStrategyNumber, F_CHAREQ, */
	/*			CharGetDatum(CONSTRAINT_FOREIGN)); */
	/*TODO */
	fkeyRel = heap_open(ConstraintRelationId, AccessShareLock);
	fkeyScan = systable_beginscan(fkeyRel, ConstraintRelidIndexId, true,
								  NULL, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(fkeyScan)))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(tuple);
		FRelEdge *currentFRelEdge = NULL;

		if (constraintForm->contype != CONSTRAINT_FOREIGN)
		{
			continue;
		}

		currentFRelEdge = palloc(sizeof(FRelEdge));
		currentFRelEdge->referencingRelationOID = constraintForm->conrelid;
		currentFRelEdge->referencedRelationOID = constraintForm->confrelid;

		frelEdgeList = lappend(frelEdgeList, currentFRelEdge);
	}

	/*
	 * Since there is no index on columns we are planning to sort tuples
	 * sorting tuples manually instead of using scan keys
	 */
	frelEdgeList = SortList(frelEdgeList, CompareFRelEdges);

	foreach(frelEdgeCell, frelEdgeList)
	{
		FRelEdge *currentFRelEdge = (FRelEdge *) lfirst(frelEdgeCell);

		/* we just saw this edge, no need to add it twice */
		if (currentFRelEdge->referencingRelationOID == prevReferencingOid &&
			currentFRelEdge->referencedRelationOID == prevReferencedOid)
		{
			continue;
		}

		AddEdge(frelGraph->nodeMap, currentFRelEdge->referencingRelationOID,
				currentFRelEdge->referencedRelationOID);

		prevReferencingOid = currentFRelEdge->referencingRelationOID;
		prevReferencedOid = currentFRelEdge->referencedRelationOID;
	}

	systable_endscan(fkeyScan);
	heap_close(fkeyRel, AccessShareLock);
}


/* Helper function to compare two FRelEdges using referencing and referenced id */
static int
CompareFRelEdges(const void *leftElement, const void *rightElement)
{
	const FRelEdge *leftEdge = *((const FRelEdge **) leftElement);
	const FRelEdge *rightEdge = *((const FRelEdge **) rightElement);

	Oid leftReferencingOID = leftEdge->referencingRelationOID;
	Oid leftReferencedOID = leftEdge->referencedRelationOID;
	Oid rightReferencingOID = rightEdge->referencingRelationOID;
	Oid rightReferencedOID = rightEdge->referencedRelationOID;

	if (leftReferencingOID < rightReferencingOID)
	{
		return 1;
	}
	else if (leftReferencedOID > rightReferencingOID)
	{
		return -1;
	}
	else
	{
		if (leftReferencedOID < rightReferencedOID)
		{
			return 1;
		}
		else if (leftReferencedOID > rightReferencedOID)
		{
			return -1;
		}
		else
		{
			return 0;
		}
	}
}


/*
 * AddEdge adds edge between the nodes having given OIDs.
 */
static void
AddEdge(HTAB *adjacencyLists, Oid referencingOid, Oid referencedOid)
{
	FRelNode *referencingNode = CreateOrFindNode(adjacencyLists, referencingOid);
	FRelNode *referencedNode = CreateOrFindNode(adjacencyLists, referencedOid);

	referencingNode->adjacencyList = lappend(referencingNode->adjacencyList,
											 referencedNode);
	referencedNode->backAdjacencyList = lappend(referencedNode->backAdjacencyList,
												referencingNode);
}


/*
 * CreateOrFindNode either gets or adds new Node to the foreign key relation graph
 */
static FRelNode *
CreateOrFindNode(HTAB *adjacencyLists, Oid relid)
{
	bool found = false;
	FRelNode *node = (FRelNode *) hash_search(adjacencyLists,
											  &relid,
											  HASH_ENTER,
											  &found);

	if (!found)
	{
		node->adjacencyList = NIL;
		node->backAdjacencyList = NIL;
		node->visited = false;
	}

	return node;
}


/*
 * ClearForeignKeyRelationGraph clear all the allocated memory obtained for
 * foreign key relation graph. Since all the variables of relation graph
 * obtained within the same context, destroying hash map is enough as it
 * deletes the context.
 */
void
ClearForeignKeyRelationGraphContext()
{
	if (frelGraph == NULL)
	{
		return;
	}

	hash_destroy(frelGraph->nodeMap);
	frelGraph = NULL;
}


/*
 * ErrorIfUnsupportedForeignConstraint runs checks related to foreign constraints and
 * errors out if it is not possible to create one of the foreign constraint in distributed
 * environment.
 *
 * To support foreign constraints, we require that;
 * - Referencing and referenced tables are hash distributed.
 * - Referencing and referenced tables are co-located.
 * - Foreign constraint is defined over distribution column.
 * - ON DELETE/UPDATE SET NULL, ON DELETE/UPDATE SET DEFAULT and ON UPDATE CASCADE options
 *   are not used.
 * - Replication factors of referencing and referenced table are 1.
 */
void
ErrorIfUnsupportedForeignConstraint(Relation relation, char distributionMethod,
									Var *distributionColumn, uint32 colocationId)
{
	Relation pgConstraint = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	HeapTuple heapTuple = NULL;

	Oid referencingTableId = relation->rd_id;
	Oid referencedTableId = InvalidOid;
	uint32 referencedTableColocationId = INVALID_COLOCATION_ID;
	Var *referencedTablePartitionColumn = NULL;

	Datum referencingColumnsDatum;
	Datum *referencingColumnArray;
	int referencingColumnCount = 0;
	Datum referencedColumnsDatum;
	Datum *referencedColumnArray;
	int referencedColumnCount = 0;
	bool isNull = false;
	int attrIdx = 0;
	bool foreignConstraintOnPartitionColumn = false;
	bool selfReferencingTable = false;

	pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);
	ScanKeyInit(&scanKey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ,
				relation->rd_id);
	scanDescriptor = systable_beginscan(pgConstraint, ConstraintRelidIndexId, true, NULL,
										scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);
		bool singleReplicatedTable = true;

		if (constraintForm->contype != CONSTRAINT_FOREIGN)
		{
			heapTuple = systable_getnext(scanDescriptor);
			continue;
		}

		referencedTableId = constraintForm->confrelid;
		selfReferencingTable = referencingTableId == referencedTableId;

		/*
		 * We do not support foreign keys for reference tables. Here we skip the second
		 * part of check if the table is a self referencing table because;
		 * - PartitionMethod only works for distributed tables and this table may not be
		 * distributed yet.
		 * - Since referencing and referenced tables are same, it is OK to not checking
		 * distribution method twice.
		 */
		if (distributionMethod == DISTRIBUTE_BY_NONE ||
			(!selfReferencingTable &&
			 PartitionMethod(referencedTableId) == DISTRIBUTE_BY_NONE))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot create foreign key constraint from or to "
								   "reference tables")));
		}

		/*
		 * ON DELETE SET NULL and ON DELETE SET DEFAULT is not supported. Because we do
		 * not want to set partition column to NULL or default value.
		 */
		if (constraintForm->confdeltype == FKCONSTR_ACTION_SETNULL ||
			constraintForm->confdeltype == FKCONSTR_ACTION_SETDEFAULT)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot create foreign key constraint"),
							errdetail("SET NULL or SET DEFAULT is not supported"
									  " in ON DELETE operation.")));
		}

		/*
		 * ON UPDATE SET NULL, ON UPDATE SET DEFAULT and UPDATE CASCADE is not supported.
		 * Because we do not want to set partition column to NULL or default value. Also
		 * cascading update operation would require re-partitioning. Updating partition
		 * column value is not allowed anyway even outside of foreign key concept.
		 */
		if (constraintForm->confupdtype == FKCONSTR_ACTION_SETNULL ||
			constraintForm->confupdtype == FKCONSTR_ACTION_SETDEFAULT ||
			constraintForm->confupdtype == FKCONSTR_ACTION_CASCADE)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot create foreign key constraint"),
							errdetail("SET NULL, SET DEFAULT or CASCADE is not"
									  " supported in ON UPDATE operation.")));
		}

		/*
		 * Some checks are not meaningful if foreign key references the table itself.
		 * Therefore we will skip those checks.
		 */
		if (!selfReferencingTable)
		{
			if (!IsDistributedTable(referencedTableId))
			{
				ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
								errmsg("cannot create foreign key constraint"),
								errdetail("Referenced table must be a distributed "
										  "table.")));
			}

			/* to enforce foreign constraints, tables must be co-located */
			referencedTableColocationId = TableColocationId(referencedTableId);
			if (colocationId == INVALID_COLOCATION_ID ||
				colocationId != referencedTableColocationId)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("cannot create foreign key constraint"),
								errdetail("Foreign key constraint can only be created"
										  " on co-located tables.")));
			}

			/*
			 * Partition column must exist in both referencing and referenced side of the
			 * foreign key constraint. They also must be in same ordinal.
			 */
			referencedTablePartitionColumn = DistPartitionKey(referencedTableId);
		}
		else
		{
			/*
			 * Partition column must exist in both referencing and referenced side of the
			 * foreign key constraint. They also must be in same ordinal.
			 */
			referencedTablePartitionColumn = distributionColumn;
		}

		/*
		 * Column attributes are not available in Form_pg_constraint, therefore we need
		 * to find them in the system catalog. After finding them, we iterate over column
		 * attributes together because partition column must be at the same place in both
		 * referencing and referenced side of the foreign key constraint
		 */
		referencingColumnsDatum = SysCacheGetAttr(CONSTROID, heapTuple,
												  Anum_pg_constraint_conkey, &isNull);
		referencedColumnsDatum = SysCacheGetAttr(CONSTROID, heapTuple,
												 Anum_pg_constraint_confkey, &isNull);

		deconstruct_array(DatumGetArrayTypeP(referencingColumnsDatum), INT2OID, 2, true,
						  's', &referencingColumnArray, NULL, &referencingColumnCount);
		deconstruct_array(DatumGetArrayTypeP(referencedColumnsDatum), INT2OID, 2, true,
						  's', &referencedColumnArray, NULL, &referencedColumnCount);

		Assert(referencingColumnCount == referencedColumnCount);

		for (attrIdx = 0; attrIdx < referencingColumnCount; ++attrIdx)
		{
			AttrNumber referencingAttrNo = DatumGetInt16(referencingColumnArray[attrIdx]);
			AttrNumber referencedAttrNo = DatumGetInt16(referencedColumnArray[attrIdx]);

			if (distributionColumn->varattno == referencingAttrNo &&
				referencedTablePartitionColumn->varattno == referencedAttrNo)
			{
				foreignConstraintOnPartitionColumn = true;
			}
		}

		if (!foreignConstraintOnPartitionColumn)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot create foreign key constraint"),
							errdetail("Partition column must exist both "
									  "referencing and referenced side of the "
									  "foreign constraint statement and it must "
									  "be in the same ordinal in both sides.")));
		}

		/*
		 * We do not allow to create foreign constraints if shard replication factor is
		 * greater than 1. Because in our current design, multiple replicas may cause
		 * locking problems and inconsistent shard contents. We don't check the referenced
		 * table, since referenced and referencing tables should be co-located and
		 * colocation check has been done above.
		 */
		if (IsDistributedTable(referencingTableId))
		{
			/* check whether ALTER TABLE command is applied over single replicated table */
			if (!SingleReplicatedTable(referencingTableId))
			{
				singleReplicatedTable = false;
			}
		}
		else
		{
			/* check whether creating single replicated table with foreign constraint */
			if (ShardReplicationFactor > 1)
			{
				singleReplicatedTable = false;
			}
		}

		if (!singleReplicatedTable)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot create foreign key constraint"),
							errdetail("Citus Community Edition currently supports "
									  "foreign key constraints only for "
									  "\"citus.shard_replication_factor = 1\"."),
							errhint("Please change \"citus.shard_replication_factor to "
									"1\". To learn more about using foreign keys with "
									"other replication factors, please contact us at "
									"https://citusdata.com/about/contact_us.")));
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	/* clean up scan and close system catalog */
	systable_endscan(scanDescriptor);
	heap_close(pgConstraint, AccessShareLock);
}


/*
 * GetTableForeignConstraints takes in a relationId, and returns the list of foreign
 * constraint commands needed to reconstruct foreign constraints of that table.
 */
List *
GetTableForeignConstraintCommands(Oid relationId)
{
	List *tableForeignConstraints = NIL;

	Relation pgConstraint = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	HeapTuple heapTuple = NULL;

	/*
	 * Set search_path to NIL so that all objects outside of pg_catalog will be
	 * schema-prefixed. pg_catalog will be added automatically when we call
	 * PushOverrideSearchPath(), since we set addCatalog to true;
	 */
	OverrideSearchPath *overridePath = GetOverrideSearchPath(CurrentMemoryContext);
	overridePath->schemas = NIL;
	overridePath->addCatalog = true;
	PushOverrideSearchPath(overridePath);

	/* open system catalog and scan all constraints that belong to this table */
	pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);
	ScanKeyInit(&scanKey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ,
				relationId);
	scanDescriptor = systable_beginscan(pgConstraint, ConstraintRelidIndexId, true, NULL,
										scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);

		if (constraintForm->contype == CONSTRAINT_FOREIGN)
		{
			Oid constraintId = get_relation_constraint_oid(relationId,
														   constraintForm->conname.data,
														   true);
			char *statementDef = pg_get_constraintdef_command(constraintId);

			tableForeignConstraints = lappend(tableForeignConstraints, statementDef);
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	/* clean up scan and close system catalog */
	systable_endscan(scanDescriptor);
	heap_close(pgConstraint, AccessShareLock);

	/* revert back to original search_path */
	PopOverrideSearchPath();

	return tableForeignConstraints;
}


/*
 * TableReferenced function checks whether given table is referenced by another table
 * via foreign constraints. If it is referenced, this function returns true. To check
 * that, this function searches given relation at pg_constraints system catalog. However
 * since there is no index for the column we searched, this function performs sequential
 * search, therefore call this function with caution.
 */
bool
TableReferenced(Oid relationId)
{
	Relation pgConstraint = NULL;
	HeapTuple heapTuple = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	Oid scanIndexId = InvalidOid;
	bool useIndex = false;

	pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_constraint_confrelid, BTEqualStrategyNumber, F_OIDEQ,
				relationId);
	scanDescriptor = systable_beginscan(pgConstraint, scanIndexId, useIndex, NULL,
										scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);

		if (constraintForm->contype == CONSTRAINT_FOREIGN)
		{
			systable_endscan(scanDescriptor);
			heap_close(pgConstraint, NoLock);

			return true;
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgConstraint, NoLock);

	return false;
}


/*
 * TableReferencing function checks whether given table is referencing by another table
 * via foreign constraints. If it is referencing, this function returns true. To check
 * that, this function searches given relation at pg_constraints system catalog. However
 * since there is no index for the column we searched, this function performs sequential
 * search, therefore call this function with caution.
 */
bool
TableReferencing(Oid relationId)
{
	Relation pgConstraint = NULL;
	HeapTuple heapTuple = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	Oid scanIndexId = InvalidOid;
	bool useIndex = false;

	pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ,
				relationId);
	scanDescriptor = systable_beginscan(pgConstraint, scanIndexId, useIndex, NULL,
										scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);

		if (constraintForm->contype == CONSTRAINT_FOREIGN)
		{
			systable_endscan(scanDescriptor);
			heap_close(pgConstraint, NoLock);

			return true;
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgConstraint, NoLock);

	return false;
}


/*
 * ConstraintIsAForeignKey returns true if the given constraint name
 * is a foreign key to defined on the relation.
 */
bool
ConstraintIsAForeignKey(char *constraintNameInput, Oid relationId)
{
	Relation pgConstraint = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	HeapTuple heapTuple = NULL;

	pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_constraint_contype, BTEqualStrategyNumber, F_CHAREQ,
				CharGetDatum(CONSTRAINT_FOREIGN));
	scanDescriptor = systable_beginscan(pgConstraint, InvalidOid, false,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);
		char *constraintName = (constraintForm->conname).data;

		if (strncmp(constraintName, constraintNameInput, NAMEDATALEN) == 0 &&
			constraintForm->conrelid == relationId)
		{
			systable_endscan(scanDescriptor);

			heap_close(pgConstraint, AccessShareLock);

			return true;
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	/* clean up scan and close system catalog */
	systable_endscan(scanDescriptor);
	heap_close(pgConstraint, AccessShareLock);

	return false;
}
