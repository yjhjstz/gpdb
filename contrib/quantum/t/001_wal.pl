# Test generic xlog record work for rum index replication.
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 31;

my $node_master;
my $node_standby;

# Run few queries on both master and standby and check their results match.
sub test_index_replay
{
    my ($test_name) = @_;

    # Check server version
    my $server_version = $node_master->safe_psql("postgres", "SELECT current_setting('server_version_num');") + 0;

    # Wait for standby to catch up
    my $applname = $node_standby->name;
    my $caughtup_query;

    if ($server_version < 100000)
    {
        $caughtup_query =
            "SELECT pg_current_xlog_location() <= write_location FROM pg_stat_replication WHERE application_name = '$applname';";
    }
    else
    {
        $caughtup_query =
            "SELECT pg_current_wal_lsn() <= write_lsn FROM pg_stat_replication WHERE application_name = '$applname';";
    }
    $node_master->poll_query_until('postgres', $caughtup_query)
      or die "Timed out while waiting for standby 1 to catch up";

    my $queries = qq(SET enable_seqscan=off;
SET enable_indexscan=on;
select id, c1 from tt where c1 ~@ ('{0.45, 0.72, 0.53, 0.14, 0.61, 0.77, 0.8, 0.59, 0.77, 0.93, 0.05, 0.86, 0.95, 0.96, 0.85, 0.04, 0.77, 0.7, 0.62, 0.75, 0.96, 0.89, 0.61, 0.07, 1, 0.57, 0.23, 0.12, 0.52, 0.74, 0.19, 0.91, 0.77, 0.05, 0, 0.23, 0.81, 0.91, 0.07, 0.14, 0.39, 0.25, 0.6, 0.6, 0.12, 0.53, 0.7, 0.07, 0.34, 0.3, 0.61, 0.73, 0.45, 1, 0.08, 0.82, 0.73, 0.99, 0.07, 0.54, 0.07, 0.04, 0.47, 0.51}', 10, 1) limit 1;
);

    # Run test queries and compare their result
    my $master_result = $node_master->psql("postgres", $queries);
    my $standby_result = $node_standby->psql("postgres", $queries);

    is($master_result, $standby_result, "$test_name: query result matches");
}

# Initialize master node
$node_master = get_new_node('master');
$node_master->init(allows_streaming => 1);
$node_master->start;
my $backup_name = 'my_backup';

# Take backup
$node_master->backup($backup_name);

# Create streaming standby linking to master
$node_standby = get_new_node('standby');
$node_standby->init_from_backup($node_master, $backup_name,
    has_streaming => 1);
$node_standby->start;

# Create some rum index on master
$node_master->psql("postgres", "CREATE EXTENSION quantum;");
$node_master->psql("postgres", "CREATE TABLE tt (id int , c1 float4[]);");
my $stmt = qq(create or replace function gen_float4_arr(int) returns float4[] as \$\$ select array_agg((random()*100)::float4) from generate_series(1,\$1); \$\$ language sql strict;);
$node_master->psql("postgres", $stmt);
$node_master->psql("postgres", "insert into tt select id, gen_float4_arr(64) from generate_series(1,100) t(id);");
$node_master->psql("postgres", "CREATE INDEX tt_idx ON tt USING quantum_hnsw (c1 ) WITH (links=4, ef=50, dims=64, efsearch=30);");

# Test that queries give same result
test_index_replay('initial');

# Run 10 cycles of table modification. Run test queries after each modification.
for my $i (1..10)
{
    $node_master->psql("postgres", "DELETE FROM tt WHERE id = $i;");
    test_index_replay("delete $i");
    $node_master->psql("postgres", "VACUUM tt;");
    test_index_replay("vacuum $i");
    my ($start, $end) = (100 + ($i - 1) * 100, 100 + $i * 100);
    $node_master->psql("postgres", "insert into tt select id, gen_float4_arr(64)
                        FROM generate_series($start,$end) t(id);");
    test_index_replay("insert $i");
}