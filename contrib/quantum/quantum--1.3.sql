/* contrib/quantum/quantum--1.3.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION quantum" to load this file. \quit



CREATE FUNCTION array_distance(float4[], float4[])
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_inner_product(float4[], float4[])
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;



-- Euclidean distance


CREATE OPERATOR <-> (
	LEFTARG = float4[], RIGHTARG = float4[], PROCEDURE = array_distance,
	COMMUTATOR = '<->'
);

CREATE OPERATOR <#> (
	LEFTARG = float4[], RIGHTARG = float4[], PROCEDURE = array_inner_product,
	COMMUTATOR = '<#>'
);



CREATE TYPE ann_params AS
(
	query     float4[],
	threshold float4,
	topk	  int4
);



CREATE OR REPLACE FUNCTION array_ann(float4[], ann_params) RETURNS boolean AS
'MODULE_PATHNAME','array_ann'
LANGUAGE C IMMUTABLE STRICT;


CREATE OPERATOR ~@ (
	LEFTARG = float4[],
	RIGHTARG = ann_params,
	PROCEDURE = array_ann,
	RESTRICT = contsel,
	JOIN = contjoinsel);



CREATE FUNCTION quantumhandler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Access method
CREATE ACCESS METHOD quantum_hnsw TYPE INDEX HANDLER quantumhandler;
COMMENT ON ACCESS METHOD quantum_hnsw IS 'hnsw index access method';

-- Opclasses
CREATE FUNCTION hnsw_stats(text, int4)
 RETURNS text
 AS 'MODULE_PATHNAME'
 LANGUAGE C STRICT;

CREATE FUNCTION linear(float4, float4)
 RETURNS float8
 AS 'MODULE_PATHNAME'
 LANGUAGE C STRICT;


CREATE OPERATOR CLASS anyarray_ops
DEFAULT FOR TYPE float4[] USING quantum_hnsw AS
	OPERATOR 1  ~@ (float4[], ann_params),
	FUNCTION 1  linear(float4, float4);

CREATE OPERATOR CLASS int4_ops
DEFAULT FOR TYPE int4 USING quantum_hnsw AS
	OPERATOR 1	= (int4, int4),
	FUNCTION 1  linear(float4, float4);

CREATE OPERATOR CLASS float4_ops
DEFAULT FOR TYPE float4 USING quantum_hnsw AS
	OPERATOR 1	= (float4, float4),
	FUNCTION 1  linear(float4, float4);

