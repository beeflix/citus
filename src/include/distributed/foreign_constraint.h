/*-------------------------------------------------------------------------
 * foreign_constraint.h
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef FOREIGN_CONSTRAINT_H
#define FOREIGN_CONSTRAINT_H

#include "postgres.h"
#include "postgres_ext.h"
#include "utils/relcache.h"
#include "utils/hsearch.h"
#include "nodes/primnodes.h"


extern void ErrorIfUnsupportedForeignConstraint(Relation relation, char
												distributionMethod,
												Var *distributionColumn, uint32
												colocationId);
extern List * GetTableForeignConstraintCommands(Oid relationId);
extern bool TableReferenced(Oid relationId);
extern bool TableReferencing(Oid relationId);
extern List * ReferencedRelationIdList(Oid relationId);
extern List * ReferencingRelationIdList(Oid relationId);
extern void SetForeignKeyGraphInvalid(void);
extern bool IsForeignKeyGraphValid(void);
extern List * ConnectedComponentOfRelationId(Oid relationId);
extern void ClearForeignKeyRelationGraphContext(void);

#endif
