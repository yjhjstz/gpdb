## HNSW中如何删除元素

一个理想的索引结构应该支持增删改查操作，由于 HNSW算法原始论文 只给出了增与查的伪代码，并没有给出删的代码，而一些项目中往往需要对已经插入到HNSW索引中的结点进行删除，比如在某些人脸识别任务中，已经1个月没有出现的人脸特征可以删除掉。下面开始分析如何在HNSW中实现删除元素操作。

首先，明确下HNSW中插入元素的过程是怎么样的。当来了一个待插入新特征，记为 x，我们先随机生成 x 所在的层次level，然后分2个步骤，1）从max_level到level+1查询离 x 最近的一个结点；2）从level到0层查询 x 最近的 maxM 个结点，然后将 x 与这些M个点建立有向连接。上述2个步骤，当需要删除元素时，就需要删除与它相连的边，第1个步骤对删除操作毫无影响，第2个步骤对删除操作的影响主要包括：1）x 在每一层有许多有向边指向 x 的邻居结点，删除 x 那么也要删除 x 所指向的边；2）x 在每一层可能是其他结点的前M个邻居结点之一，设该结点为 b，当删除 x时，应该删除由 b 出现指向 x 的有向边。

当我们需要实现删除操作时，那么必须要检查每一层中所有结点的maxM个邻居是否因为删除 x 而发生改变，如果发生改变，那么就需要重建该结点的maxM邻居信息。这个工作非常耗时，要达到此目的，我这有2个办法。一是，采用lazy模式，实际上不删除 x，而是把x加入到黑名单中，下次搜索时判断是否在黑名单中而决定是否返回x，当黑名单数超过一定阈值时，重建HNSW索引；二是采用空间换时间办法，当把x插入时就在x的邻居信息中记住 x 是哪些结点的邻居。


* 记录入度的问题：入度最大是多少，无法提前预知，可能是某层所有的元素。
* lazy 模式可考虑，但也不完美。
* 考虑分区删除。
* 全量扫描删除关联节点的出度，重建


原文链接：https://blog.csdn.net/CHIERYU/article/details/86647014

## conf

> if you are on RAID 10, the cost of accessing a random page should be much closer to the cost
  of sequential IO. (In fact, if you are on SSDs, seq_page_cost and random_page_cost should equal each other.)

random_page_cost = 4.0  ==>  random_page_cost = 2.0

max_worker_processes = 48;


## Debug 编译
./configure --with-blocksize=32 --enable-debug --enable-cassert --enable-thread-safety CFLAGS='-O0 -g'



## 主备大小
* SELECT * FROM pg_stat_progress_create_index ;
* CREATE ROLE replica login replication encrypted password 'replica';
* pg_basebackup -D $PGDATA -Fp -Xstream -R -c fast -v -P -h 10.186.1.156 -p 8032 -U replica -W
* vacuum FULL VERBOSE ANALYZE tt;
* pg_dump -h 116.85.13.173 -p 5900 -U postgres -t sift -d pg | psql -h localhost -d pg -U luban




## index warmup
* select pg_prewarm('sift_idx_32', 'read', 'main');




