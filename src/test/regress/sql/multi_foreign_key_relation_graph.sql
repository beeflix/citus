SET citus.next_shard_id TO 3000000;
SET citus.shard_replication_factor TO 1;

CREATE FUNCTION get_referencing_relation_id_list(Oid)
    RETURNS SETOF Oid
    LANGUAGE C STABLE STRICT
    AS 'citus', $$get_referencing_relation_id_list$$;

CREATE FUNCTION get_referenced_relation_id_list(Oid)
    RETURNS SETOF Oid
    LANGUAGE C STABLE STRICT
    AS 'citus', $$get_referenced_relation_id_list$$;

-- Simple case with distributed tables
CREATE TABLE dtt1(id int PRIMARY KEY);
SELECT create_distributed_table('dtt1','id');

CREATE TABLE dtt2(id int PRIMARY KEY REFERENCES dtt1(id));
SELECT create_distributed_table('dtt2','id');

CREATE TABLE dtt3(id int PRIMARY KEY REFERENCES dtt2(id));
SELECT create_distributed_table('dtt3','id');

SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('dtt1'::regclass) ORDER BY 1;
SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('dtt2'::regclass) ORDER BY 1;
SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('dtt3'::regclass) ORDER BY 1;

SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt1'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt2'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt3'::regclass) ORDER BY 1;


CREATE TABLE dtt4(id int PRIMARY KEY);
SELECT create_distributed_table('dtt4', 'id');

SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('dtt4'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt4'::regclass) ORDER BY 1;

ALTER TABLE dtt4 ADD CONSTRAINT dtt4_fkey FOREIGN KEY (id) REFERENCES dtt3(id);

SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('dtt4'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt4'::regclass) ORDER BY 1;

SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('dtt1'::regclass) ORDER BY 1;
SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('dtt2'::regclass) ORDER BY 1;
SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('dtt3'::regclass) ORDER BY 1;

SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt1'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt2'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt3'::regclass) ORDER BY 1;

ALTER TABLE dtt4 DROP CONSTRAINT dtt4_fkey;

SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('dtt3'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt3'::regclass) ORDER BY 1;

SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('dtt4'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt4'::regclass) ORDER BY 1;
