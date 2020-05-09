create extension if not exists quantum;

create table tt (id int , c1 float4[]);

\copy tt FROM 'data/array.csv' WITH csv;

CREATE INDEX tt_idx ON tt USING quantum_hnsw (c1 ) WITH (m=4, efbuild=50, dims=64, efsearch=30);

alter table tt set (parallel_workers = 16);

set max_parallel_workers_per_gather = 4;

set session enable_seqscan=false;


select id from tt where c1 ~@ ('{0.45, 0.72, 0.53, 0.14, 0.61, 0.77, 0.8, 0.59, 0.77, 0.93, 0.05, 0.86, 0.95, 0.96, 0.85, 0.04, 0.77, 0.7, 0.62, 0.75, 0.96, 0.89, 0.61, 0.07, 1, 0.57, 0.23, 0.12, 0.52, 0.74, 0.19, 0.91, 0.77, 0.05, 0, 0.23, 0.81, 0.91, 0.07, 0.14, 0.39, 0.25, 0.6, 0.6, 0.12, 0.53, 0.7, 0.07, 0.34, 0.3, 0.61, 0.73, 0.45, 1, 0.08, 0.82, 0.73, 0.99, 0.07, 0.54, 0.07, 0.04, 0.47, 0.51}', 10, 1) limit 1;

set session enable_seqscan=true;
select id from tt order by c1 <-> array[0.45, 0.72, 0.53, 0.14, 0.61, 0.77, 0.8, 0.59, 0.77, 0.93, 0.05, 0.86, 0.95, 0.96, 0.85, 0.04, 0.77, 0.7, 0.62, 0.75, 0.96, 0.89, 0.61, 0.07, 1, 0.57, 0.23, 0.12, 0.52, 0.74, 0.19, 0.91, 0.77, 0.05, 0, 0.23, 0.81, 0.91, 0.07, 0.14, 0.39, 0.25, 0.6, 0.6, 0.12, 0.53, 0.7, 0.07, 0.34, 0.3, 0.61, 0.73, 0.45, 1, 0.08, 0.82, 0.73, 0.99, 0.07, 0.54, 0.07, 0.04, 0.47, 0.51] limit 1;

drop index tt_idx;