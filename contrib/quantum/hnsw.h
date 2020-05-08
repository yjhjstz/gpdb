/*-------------------------------------------------------------------------
 *
 * hnsw.h
 *	  Header for hnsw index.
 *
 * Copyright (c) 2016-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/quantum/hnsw.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _HNSW_H_
#define _HNSW_H_

#include "access/amapi.h"
#include "access/generic_xlog.h"
#include "access/itup.h"
#include "access/xlog.h"


#if PG_VERSION_NUM < 120000
#include "nodes/relation.h"
#else
#include "nodes/pathnodes.h"
#endif
#include "storage/s_lock.h"
#include "lib/binaryheap.h"
#include "lib/pairingheap.h"

#include "fmgr.h"
#include "util.h"


#if PG_VERSION_NUM >= 120000
#define GET_SCAN_TID(scan)      ((scan)->xs_heaptid)
#define SET_SCAN_TID(scan, tid) ((scan)->xs_heaptid = (tid))
#else
#define GET_SCAN_TID(scan)      ((scan)->xs_ctup.t_self)
#define SET_SCAN_TID(scan, tid) ((scan)->xs_ctup.t_self = (tid))
#endif

typedef float storageType;
typedef float (*dist_func)(const float *x, const float *y, size_t d);

/* Support procedures numbers */
#define HNSW_DISTANCE_PROC     1
#define HNSW_NPROC             1

#define ALLOCSET_LARGE_MAXSIZE (64 * 1024 * 1024)
#define ALLOCSET_LARGE_SIZES \
    ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_LARGE_MAXSIZE

#define ARRPTR(x)  ( (float *) ARR_DATA_PTR(x) )
#define ARRNELEMS(x)  ArrayGetNItems( ARR_NDIM(x), ARR_DIMS(x))

/* GUC parameters */
extern PGDLLIMPORT int      index_parallel;
extern PGDLLIMPORT bool link_nearest;

/* Opaque for hnsw pages */
typedef struct HnswPageOpaqueData
{
	BlockNumber hnsw_prevblkno;	/* link */
	BlockNumber hnsw_nextblkno;	/* link */
	int16		level;	       /* level number  */
	OffsetNumber    maxoff;         /* number of index tuples on page */
	uint16		flags;		/* page type code + flag bits, see above */
	uint16		hnsw_page_id;	/* for identification of HNSW indexes */

} HnswPageOpaqueData;

typedef HnswPageOpaqueData *HnswPageOpaque;

/* Hnsw page flags */
#define HNSW_OVERFLOW		(1 << 0)
#define HNSW_BUCKET		(1 << 1)
#define HNSW_META			(1 << 2)
#define HNSW_DELETED        (1 << 3)
#define MAX_LEVEL 8
#define INVALID_LEVEL -1

#define HNSW_PAGE_TYPE \
	(HNSW_OVERFLOW | HNSW_BUCKET | HNSW_META)
/*
 * The page ID is for the convenience of pg_filedump and similar utilities,
 * which otherwise would have a hard time telling pages of different index
 * types apart.  It should be the last 2 bytes on the page.  This is more or
 * less "free" due to alignment considerations.
 *
 * See comments above GinPageOpaqueData.
 */
#define HNSW_PAGE_ID		0xFF84

/* Macros for accessing bloom page structures */
#define HnswPageGetOpaque(page) ((HnswPageOpaque) PageGetSpecialPointer(page))
#define HnswPageGetMaxOffset(page) (HnswPageGetOpaque(page)->maxoff)
#define HnswPageIsMeta(page) \
	((HnswPageGetOpaque(page)->flags & HNSW_META) != 0)
#define HnswPageIsBucket(page) \
	((HnswPageGetOpaque(page)->flags & HNSW_BUCKET) != 0)
#define HnswPageIsOverflow(page) \
	((HnswPageGetOpaque(page)->flags & HNSW_OVERFLOW) != 0)
#define HnswPageIsDeleted(page) \
	((HnswPageGetOpaque(page)->flags & HNSW_DELETED) != 0)
#define HnswPageSetDeleted(page) \
	(HnswPageGetOpaque(page)->flags |= HNSW_DELETED)
#define HnswPageSetNonDeleted(page) \
	(HnswPageGetOpaque(page)->flags &= ~HNSW_DELETED)



/* Preserved page numbers */
#define HNSW_METAPAGE_BLKNO	(0)
#define HNSW_BUCKET_BLKNOL1  (1) /* first bucket page */
#define HNSW_NOLOCK		(-1)


/* Hnsw index options */
typedef struct HnswOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int	        max_links;
   	int         ef_construction; 	
	int         dims;
	int 		efsearch;
	char        algo[0];
} HnswOptions;

enum DISTANCE_FUNC
{
    L2_DIST = 1,
    DOT_DIST = 2,
    CUSTOM_DOT_DIST = 3,
};
/* Metadata of HNSW index */
typedef struct HnswMetaPageData
{
	uint32		magic;
	uint32      version;
	uint16		dims;
	uint16      M_; /* 每个点需要与图中其他的点建立的连接数 */
	uint16      maxM_; /* 最大的连接数 */
	uint16      maxM0_; /* level 0 的最大连接数 */
	uint16      ef_construction_; /* 动态候选元素集合大小 */
	uint16      efsearch_; /*ef search queue*/
	uint16      atrrnum; /* cube atrr num to get cube */
	int       	max_level_; /* 最大层数 */
	uint32		search_count; /*查询统计*/
	int 		nproc; /*l2:1 or dot:2*/
	BlockNumber level_blk[MAX_LEVEL];	/* blknos of bucket */
} HnswMetaPageData;

typedef HnswMetaPageData *HnswMetaPage;


typedef struct HnswBucketData
{
    uint32			level;	       /* level number  */
    uint32 		dims;	/*dims*/
    uint32          ntuples;
    uint32          pages;
    ItemPointerData entry; /* 每level 有入口，便于并发*/
    /* 跟踪空闲，使用链表 */
    BlockNumber     first_free;
    BlockNumber     first_full;
} HnswBucketData;


/* Magic number to distinguish bloom pages among anothers */
#define HNSW_MAGICK_NUMBER (0xDBAC9527)


#define HnswPageGetMeta(page)	((HnswMetaPageData *) PageGetContents(page))
#define HnswPageGetBucket(page) ((HnswBucketData *) PageGetContents(page))

typedef struct HnswState
{
    FmgrInfo    distanceFn[INDEX_MAX_KEYS];
    Oid         collations[INDEX_MAX_KEYS];
	Relation	index;
	bool		isBuild;
	int32		ncolumns;
	uint16      atrrnum; // # of cube
	TupleDesc	tupdesc;
	int	        max_links;
   	int         ef_construction; 	
   	int 		efsearch;
	int         dims;
    int         nproc;
	/*
	 * sizeOfHnswTuple is index-specific, and it depends on reloptions, so
	 * precompute it
	 */
	Size		sizeOfHnswTuple;
	
} HnswState;

#define HnswPageGetFreeSpace(state, page) \
	(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) \
		- HnswPageGetMaxOffset(page) * (state)->sizeOfHnswTuple \
		- MAXALIGN(sizeof(HnswPageOpaqueData)))


typedef struct HnswNode {
     //HeapTuple htup; // store array data
     ItemPointerData pointer;	/* redirection inside index */
     float    distance;  // distance L2 of (q, tid)
} HnswNode;
/*
 * Tuples are very different from all other relations
 */
typedef struct HnswTuple
{
        ItemPointerData heapPtr; // in heap, for bitmap scan
        uint32		level;
        uint32      maxM;
        uint32      dims;
        uint32      out_degree;
        uint32      in_degree;
	
        uint32      offset_out_links;
        uint32      id; 	/* id column */
        float       bias;   /* bias columm */
        Size        size_tuple;  // size of tuple
        bool        deleted;
        /* heap block number that the tuple in next level */
		ItemPointerData next; // in hnsw index
		ItemPointerData iptr;

		Buffer      buf;
        slock_t     mutex; // guard outgoing 
        storageType  x[FLEXIBLE_ARRAY_MEMBER];

        //HnswNode outgoing[FLEXIBLE_ARRAY_MEMBER];
} HnswTuple;



typedef struct HnswSearchItem
{
	pairingheap_node phNode;	/* pairing heap node */
	HnswNode*		value;			/* value reconstructed from parent */
	
	int 	ref;
} HnswSearchItem;


#define HNSWTUPLEHDRSZ offsetof(HnswTuple, x)
#define HnswGetTupleNodes(tuple) \
	(HnswNode*)((char*)tuple + tuple->offset_out_links)




/* Opaque data structure for hnsw index scan */
typedef struct HnswScanOpaqueData
{
	HnswState state;
	Oid		   *orderByTypes;	/* datatypes of ORDER BY expressions */
	pairingheap *queue;			/* queue of unvisited items */
	MemoryContext tempCxt;		/* for rescan key */
	MemoryContext queueCxt;		/* context holding the queue */
    storageType* q;             /* query vector */
    int   dims;                 /* query vector dims */
    float threshold;
    int topk;
    int count;
    bool first_call;

} HnswScanOpaqueData;

typedef HnswScanOpaqueData *HnswScanOpaque;

#define print(pre, i, iptr) elog(INFO, pre"%d- (%d,%d)", i, ItemPointerGetBlockNumber(iptr), ItemPointerGetOffsetNumber(iptr));

/* hnutils.c */
extern void _PG_init(void);
extern void initHnswState(HnswState *state, Relation index);
extern Buffer _addfirstpage(GenericXLogState *state, Relation index, Page metapage, BlockNumber mblk, bool isBuild, Page* ovflpage);
extern Buffer _addovflpage(GenericXLogState *state, Relation index, Page metapage, Buffer buf, bool retain_pin, bool);
extern OffsetNumber _hnsw_pgaddtup(Relation rel, Page page, Size itemsize, HnswTuple* itup);
extern HnswTuple *HnswFormTuple(HnswState *state, ItemPointer iptr, Datum *values, bool *isnull, int);

/* hnpage.c */
extern void HnswInitMetapage(Relation index, bool isBuild);
extern Buffer HnswNewBuffer(Relation index);
extern void HnswUpdateMetapage(Relation index, int maxlevel, bool isBuild);

extern HnswMetaPage _getcachedmetap(Relation rel, Buffer *metabuf, bool force_refresh);
extern void _hnsw_relbuf(Relation rel, Buffer buf);
extern void _hnsw_dropbuf(Relation rel, Buffer buf);
extern Buffer _hnsw_getbuf(Relation rel, BlockNumber blkno, int access);
extern void _hnsw_initbuf(Page page, uint32 flags, int16 level, bool initpage);
/* blvalidate.c */
extern bool blvalidate(Oid opclassoid);

/* index access method interface functions */
extern bool blinsert(Relation index, Datum *values, bool *isnull,
		 ItemPointer ht_ctid, Relation heapRel,
		 IndexUniqueCheck checkUnique);
extern IndexScanDesc blbeginscan(Relation r, int nkeys, int norderbys);
extern int64 blgetbitmap(IndexScanDesc scan, TIDBitmap *tbm);
extern bool blgettuple(IndexScanDesc scan, ScanDirection dir);
extern void blrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		 ScanKey orderbys, int norderbys);
extern void blendscan(IndexScanDesc scan);
extern IndexBuildResult *blbuild(Relation heap, Relation index,
		struct IndexInfo *indexInfo);
extern void blbuildempty(Relation index);
extern IndexBulkDeleteResult *blbulkdelete(IndexVacuumInfo *info,
			 IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback,
			 void *callback_state);
extern IndexBulkDeleteResult *blvacuumcleanup(IndexVacuumInfo *info,
				IndexBulkDeleteResult *stats);
extern bytea *bloptions(Datum reloptions, bool validate);
#if 0
extern void blcostestimate(PlannerInfo *root, IndexPath *path,
			   double loop_count, Cost *indexStartupCost,
			   Cost *indexTotalCost, Selectivity *indexSelectivity,
			   double *indexCorrelation, double *indexPages);
#endif
extern void blcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
               Cost *indexStartupCost, Cost *indexTotalCost,
               Selectivity *indexSelectivity, double *indexCorrelation);

/* hnutil.c */

extern ItemPointerData _greedy_search(HnswState *state, size_t ntuples, void* q, ItemPointer ep);
extern binaryheap* _search_level(HnswState *state, size_t ef, size_t level, void* q, ItemPointer ep);
extern void bidirection_connect(HnswState *state, binaryheap* candidates, HnswTuple* cur, bool isBuild);
extern void bidirection_connect_simple(HnswState* state, binaryheap* candidates, HnswTuple* cur, bool isBuild);
extern void  bidirection_connect2(HnswState* state, binaryheap* candidates, ItemPointerData iptr, int lv, bool isBuild);
extern bool _updateHnswTuple(Relation index, ItemPointer ptr, HnswTuple* tuple, bool isBuild);
extern HnswTuple* _getHnswTuple(HnswState *state, ItemPointer ptr);
extern storageType* _getTupleArray(Relation rel, BlockNumber blk, OffsetNumber offnum);
/* util.cc c++ wrap*/

extern size_t random_level(int max_links);
extern ItemPointerSet stlset_create(void);
extern bool stlset_add_member(ItemPointerSet *stlset, int64 p);
extern bool stlset_is_member(ItemPointerSet *stlset, int64 p);
extern void stlset_release(ItemPointerSet *stlset);


extern float fvec_L2sqr (const float * x, const float * y, size_t d);
extern float fvec_inner_product (const float * x, const float * y, size_t d);
//extern float fvec_L2sqr_avx(const float * x, const float * y, size_t d);


#endif
