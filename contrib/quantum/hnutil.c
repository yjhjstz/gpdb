/*-------------------------------------------------------------------------
 *
 * hnutils.c
 *		Hnsw index utilities.
 *
 * Portions Copyright (c) 2016-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1990-1993, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/quantum/hnutils.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdlib.h>

#include "access/amapi.h"
#include "access/generic_xlog.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#if PG_VERSION_NUM < 120000
#include "catalog/pg_type.h"
#include "storage/spin.h"
#else
#include "catalog/pg_type_d.h"
#endif
#include "storage/lmgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "utils/memutils.h"
#include "utils/selfuncs.h"
#include "utils/guc.h"
#include "access/reloptions.h"
#include "storage/freespace.h"
#include "storage/indexfsm.h"


#include "hnsw.h"
#include "util.h"


PG_FUNCTION_INFO_V1(quantumhandler);

//#define DEBUG
/* Kind of relation options for bloom index */
static relopt_kind bl_relopt_kind;
/* parse table for fillRelOptions */
static relopt_parse_elt bl_relopt_tab[5];

dist_func select_distfunc(int nproc);
float _compute_distance_custom(HnswState* s, float d, float bias);
HnswSearchItem *hnswAllocSearchItem(ItemPointer pt, float dist);

int
pairingheap_HnswSearchItem_cmp(const pairingheap_node *a,
							   const pairingheap_node *b, void *arg)
{
	const		HnswSearchItem *sa = (const HnswSearchItem *) a;
	const		HnswSearchItem *sb = (const HnswSearchItem *) b;

	if (sa->value->distance != sb->value->distance)
		return (sa->value->distance < sb->value->distance) ? 1 : -1;
	
	
	return 0;
}

void
hnswFreeSearchItem(HnswSearchItem * item)
{
	// if (DatumGetPointer(item->value) != NULL &&
	// 	item->ref == 0)
	// 	pfree(DatumGetPointer(item->value));

	pfree(item);
}


HnswSearchItem *
hnswAllocSearchItem(ItemPointer pt, float dist)
{
	HnswSearchItem *item = palloc0( sizeof(HnswSearchItem));
	HnswNode* newnode = (HnswNode*) palloc0(sizeof(HnswNode));
	newnode->distance = dist;
	ItemPointerCopy(pt, &newnode->pointer);
	item->value = newnode;
	item->ref = 0;
	return item;
}


/*
 * Module initialize function: initialize info about Bloom relation options.
 *
 * Note: keep this in sync with makeDefaultBloomOptions().
 */
void
_PG_init(void)
{
	if (BLCKSZ != 32768)
		ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR),
			errmsg("postgres must be config BLCKSZ to 32768.")));
	/* Define custom GUC variables. */
	DefineCustomIntVariable("quantum.index_parallel",
				  "Sets the maximum allowed worker for build L0 index.",
							NULL,
							&index_parallel,
							0, 0, 20,
							PGC_USERSET, 0,
							NULL, NULL, NULL);

	DefineCustomBoolVariable("quantum.link_nearest",
							 "link nearest neighbours.",
							 NULL,
							 &link_nearest,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	bl_relopt_kind = add_reloption_kind();

	/* Option for length of signature */
	add_int_reloption(bl_relopt_kind, "m",
					  "max links of neighbours",
					  16, 1, 64);
	bl_relopt_tab[0].optname = "m";
	bl_relopt_tab[0].opttype = RELOPT_TYPE_INT;
	bl_relopt_tab[0].offset = offsetof(HnswOptions, max_links);


	/* Option for length of signature */
	add_int_reloption(bl_relopt_kind, "efbuild",
					  "construction queue length",
					  128, 1, 500);
	bl_relopt_tab[1].optname = "efbuild";
	bl_relopt_tab[1].opttype = RELOPT_TYPE_INT;
	bl_relopt_tab[1].offset = offsetof(HnswOptions, ef_construction);

	/* Option for length of signature */
	add_int_reloption(bl_relopt_kind, "dims",
					  "dims of vector data",
					  64, 1, 4096);
	bl_relopt_tab[2].optname = "dims";
	bl_relopt_tab[2].opttype = RELOPT_TYPE_INT;
	bl_relopt_tab[2].offset = offsetof(HnswOptions, dims);

	/* Option for length of signature */
	add_int_reloption(bl_relopt_kind, "efsearch",
					  "ef of search",
					   64, 1, 1024);
	bl_relopt_tab[3].optname = "efsearch";
	bl_relopt_tab[3].opttype = RELOPT_TYPE_INT;
	bl_relopt_tab[3].offset = offsetof(HnswOptions, efsearch);

	/* Option for length of signature */
	add_string_reloption(bl_relopt_kind, "algorithm",
					  "algorithm of compute distance",
					  "l2", NULL);
	bl_relopt_tab[4].optname = "algorithm";
	bl_relopt_tab[4].opttype = RELOPT_TYPE_STRING;
	bl_relopt_tab[4].offset = 0;


	
}





/*
 * hnsw handler function: return IndexAmRoutine with access method parameters
 * and callbacks.
 */
Datum
quantumhandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = 0;
	amroutine->amsupport = HNSW_NPROC;
	amroutine->amcanorder = true;
	amroutine->amcanorderbyop = true;
	amroutine->amcanbackward = false;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
#if PG_VERSION_NUM >= 100000
	amroutine->amcanparallel = false;
	amroutine->amcaninclude = false;
#endif
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = blbuild;
	amroutine->ambuildempty = blbuildempty;
	amroutine->aminsert = blinsert;
	amroutine->ambulkdelete = blbulkdelete;
	amroutine->amvacuumcleanup = blvacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = blcostestimate;
	amroutine->amoptions = bloptions;
	amroutine->amproperty = NULL;
	//amroutine->ambuildphasename = NULL;
	amroutine->amvalidate = blvalidate;
	amroutine->ambeginscan = blbeginscan;
	amroutine->amrescan = blrescan;
	amroutine->amgettuple = blgettuple;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = blendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
#if PG_VERSION_NUM >= 100000
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;
#endif
	PG_RETURN_POINTER(amroutine);
}

/*
 * Fill HnswState structure for particular index.
 */
void
initHnswState(HnswState *state, Relation index)
{
	int			i;
	
	HnswMetaPageData *metap;
	state->index = index;
	state->isBuild = false;
	state->ncolumns = index->rd_att->natts;
	state->tupdesc = index->rd_att; 
	/* Initialize hash function for each attribute */
	for (i = 0; i < index->rd_att->natts; i++)
	{
		if (TupleDescAttr(index->rd_att, i)->atttypid == FLOAT4ARRAYOID) {
			state->atrrnum = i + 1;
			//break;
		}
		fmgr_info_copy(&(state->distanceFn[i]),
					   index_getprocinfo(index, i + 1, HNSW_DISTANCE_PROC),
					   CurrentMemoryContext);
		state->collations[i] = index->rd_indcollation[i];

	}
	if (state->atrrnum < 1) 
		elog(ERROR, "atrrnum %d, %d\n", state->atrrnum, state->ncolumns);
	/* Initialize amcache if needed with options from metapage */
	if (!index->rd_amcache)
	{
		Buffer		buffer;
		Page		page;
		HnswMetaPageData *meta;
		char	*cache;

		cache = MemoryContextAlloc(index->rd_indexcxt, sizeof(HnswMetaPageData));

		buffer = _hnsw_getbuf(index, HNSW_METAPAGE_BLKNO, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buffer);

		if (!HnswPageIsMeta(page))
			elog(ERROR, "Relation is not a hnsw index");
		meta = HnswPageGetMeta(BufferGetPage(buffer));

		if (meta->magic != HNSW_MAGICK_NUMBER)
			elog(ERROR, "Relation is not a hnsw index");

		if (index->rd_amcache == NULL)
			index->rd_amcache = cache;
		memcpy(index->rd_amcache, HnswPageGetMeta(page),
			   sizeof(HnswMetaPageData));
		_hnsw_relbuf(index, buffer);
	}

	metap = (HnswMetaPage)index->rd_amcache;
	state->max_links = metap->maxM_;
	state->ef_construction = metap->ef_construction_;
	state->efsearch = metap->efsearch_;
	state->dims = metap->dims;
	state->nproc = metap->nproc;

	state->sizeOfHnswTuple = HNSWTUPLEHDRSZ +
		2 * sizeof(HnswNode) * state->max_links + sizeof(storageType) * state->dims;


}



/*
 * Make hnsw tuple from values.
 */
HnswTuple *
HnswFormTuple(HnswState *state, ItemPointer iptr, Datum *values, bool *isnull, int lv)
{
	int			i;
	ArrayType* 	arr;
	float* dur;
	HnswTuple *res = (HnswTuple *) palloc0(state->sizeOfHnswTuple);

	res->heapPtr = *iptr;

	res->level = lv;
	res->maxM = (lv == 0) ? 2* state->max_links : state->max_links;
	res->dims = state->dims;
	res->out_degree = 0;
	res->in_degree = 0;
	res->offset_out_links = HNSWTUPLEHDRSZ + sizeof(storageType) * state->dims;
	res->bias = 0.0;
	res->size_tuple = state->sizeOfHnswTuple;
	res->deleted = false;
	for (i = 0; i < state->ncolumns; i++)
	{
		/* skip nulls */
		if (isnull[i])
			continue;
		if (i == state->atrrnum - 1) {
			arr = DatumGetArrayTypeP(values[i]);
			dur = ARRPTR(arr);
			if (ARRNELEMS(arr) != state->dims) {
				elog(FATAL, "dims not the same %d, %d", ARRNELEMS(arr), state->dims);
			}
			memcpy(res->x, dur, sizeof(storageType) * state->dims);
			// for (j = 0; j < res->dims; j++) {
			// 	res->x[j] = (dur[j]);
			// } 
		} else if (TupleDescAttr(state->index->rd_att, i)->atttypid == FLOAT4OID) {
			res->bias = DatumGetFloat4(values[i]);
		} else if (TupleDescAttr(state->index->rd_att, i)->atttypid == INT4OID) {
			res->id = DatumGetInt32(values[i]);
		}
	}
	SpinLockInit(&res->mutex);
	return res;
}





/*
 *	_hnsw_pgaddtup() -- add a tuple to a particular page in the index.
 *
 * This routine adds the tuple to the page as requested; it does not write out
 * the page.  It is an error to call pgaddtup() without pin and write lock on
 * the target buffer.
 *
 * Returns the offset number at which the tuple was inserted.  This function
 * is responsible for preserving the condition that tuples in a hash index
 * page are sorted by hashkey value.
 */

OffsetNumber
_hnsw_pgaddtup(Relation rel, Page page, Size itemsize, HnswTuple* itup)
{
	OffsetNumber itup_off;

	itup_off = PageAddItem(page, (Item) itup, itemsize, InvalidOffsetNumber, false, false);
	if (itup_off == InvalidOffsetNumber)
		elog(ERROR, "failed to add index item to \"%s\"",
			 RelationGetRelationName(rel));

	return itup_off;
}


/*
 * Parse reloptions for bloom index, producing a BloomOptions struct.
 */
bytea *
bloptions(Datum reloptions, bool validate)
{
	relopt_value *options;
	int			numoptions;
	HnswOptions *rdopts;

	/* Parse the user-given reloptions */
	options = parseRelOptions(reloptions, validate, bl_relopt_kind, &numoptions);
	rdopts = allocateReloptStruct(sizeof(HnswOptions), options, numoptions);
	fillRelOptions((void *) rdopts, sizeof(HnswOptions), options, numoptions,
				   validate, bl_relopt_tab, lengthof(bl_relopt_tab));

	//elog(INFO, "bloptions:%d, %d, %d, %d\n",\
	//     rdopts->max_links, rdopts->ef_construction, rdopts->dims, rdopts->efsearch);
	return (bytea *) rdopts;
}


void blcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
               Cost *indexStartupCost, Cost *indexTotalCost,
               Selectivity *indexSelectivity, double *indexCorrelation) {
	IndexOptInfo *index = path->indexinfo;
	List	   *qinfos;
	GenericCosts costs;

	/* Do preliminary analysis of indexquals */
	qinfos = deconstruct_indexquals(path);

	MemSet(&costs, 0, sizeof(costs));

	/* We have to visit all index tuples anyway */
	costs.numIndexTuples = index->tuples * 0.3;

	/* Use generic estimate */
	genericcostestimate(root, path, loop_count, qinfos, &costs);

	*indexStartupCost = costs.indexStartupCost;
	*indexTotalCost = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
}

#if 0
void
blcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
			   Cost *indexStartupCost, Cost *indexTotalCost,
			   Selectivity *indexSelectivity, double *indexCorrelation,
			   double *indexPages)
{
	IndexOptInfo *index = path->indexinfo;
	GenericCosts costs;

	MemSet(&costs, 0, sizeof(costs));

	/* We have to visit 30% index tuples  */
	costs.numIndexTuples = index->tuples * 0.3;

	/* Use generic estimate */
	genericcostestimate(root, path, loop_count, &costs);

	*indexStartupCost = costs.indexStartupCost;
	*indexTotalCost = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
	*indexPages = costs.numIndexPages;
	//elog(INFO, "## %f, %f, %f", costs.numIndexTuples, costs.numIndexPages, index->tuples);
}
#endif



Buffer
_addovflpage(GenericXLogState *state, Relation index, Page metapage, Buffer buf, bool retain_pin, bool isBuild)
{
	Buffer		ovflbuf;
	Page		page;
	Page		ovflpage;
	HnswPageOpaque pageopaque;
	HnswPageOpaque ovflopaque;
	HnswBucketData* metap;
	BlockNumber blkno;
	//GenericXLogState *state;
	
	/*
	 * Write-lock the tail page.  Here, we need to maintain locking order such
	 * that, first acquire the lock on tail page of bucket, then on meta page
	 * to find and lock the bitmap page and if it is found, then lock on meta
	 * page is released, then finally acquire the lock on new overflow buffer.
	 * We need this locking order to avoid deadlock with backends that are
	 * doing inserts.
	 *
	 * Note: We could have avoided locking many buffers here if we made two
	 * WAL records for acquiring an overflow page (one to allocate an overflow
	 * page and another to add it to overflow bucket chain).  However, doing
	 * so can leak an overflow page, if the system crashes after allocation.
	 * Needless to say, it is better to have a single record from a
	 * performance point of view as well.
	 */
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	/* loop to find current tail page, in case someone else inserted too */
	for (;;)
	{

		BlockNumber nextblkno;
		page = BufferGetPage(buf);
		pageopaque = (HnswPageOpaque) PageGetSpecialPointer(page);
		nextblkno = pageopaque->hnsw_nextblkno;

		//elog(INFO, "find tail page nextblkno %d ", nextblkno);
		if (!BlockNumberIsValid(nextblkno))
			break;

		if (retain_pin)
		{
			/* pin will be retained only for the primary bucket page */
			Assert((pageopaque->flags & HNSW_PAGE_TYPE) == HNSW_BUCKET);
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		} else {
			UnlockReleaseBuffer(buf);
		}

		retain_pin = false;

		buf = ReadBuffer(index, nextblkno);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	}
	
	if (isBuild)
		page = BufferGetPage(buf);
	else
		page = GenericXLogRegisterBuffer(state, buf, 0);
	pageopaque = (HnswPageOpaque) PageGetSpecialPointer(page);

	metap = HnswPageGetBucket(metapage);

	/* lock overflow buf*/
	ovflbuf = HnswNewBuffer(index);	
	if (isBuild) {
		ovflpage = BufferGetPage(ovflbuf);
	} else {
		ovflpage = GenericXLogRegisterBuffer(state, ovflbuf, GENERIC_XLOG_FULL_IMAGE);
	}

	_hnsw_initbuf(ovflpage, HNSW_OVERFLOW, metap->level, true);
	
    metap->pages += 1;
	/*
	 * Adjust hashm_firstfree to avoid redundant searches.  But don't risk
	 * changing it if someone moved it while we were searching bitmap pages.
	 */
	blkno = BufferGetBlockNumber(ovflbuf);
	
	metap->first_free = blkno;
	
	/* initialize new overflow page */
	//ovflpage = BufferGetPage(ovflbuf);
	ovflopaque = (HnswPageOpaque) PageGetSpecialPointer(ovflpage);
	ovflopaque->hnsw_prevblkno = BufferGetBlockNumber(buf);
	ovflopaque->hnsw_nextblkno = InvalidBlockNumber;
	ovflopaque->level = metap->level;
	ovflopaque->flags = HNSW_OVERFLOW;
	ovflopaque->hnsw_page_id = HNSW_PAGE_ID;
	
	/* logically chain overflow page to previous page */
	pageopaque->hnsw_nextblkno = blkno;
	if (isBuild) {
		MarkBufferDirty(ovflbuf);
		MarkBufferDirty(buf);
	} else {
		//GenericXLogFinish(state);
	}

	//_hnsw_relbuf(index, buf);
	
	return ovflbuf;
}

Buffer
_addfirstpage(GenericXLogState *state, Relation index, Page metapage, BlockNumber mblk, bool isBuild, Page* ovflpage)
{
	Buffer		ovflbuf;
	//Page		ovflpage;
	HnswPageOpaque pageopaque;
	HnswPageOpaque ovflopaque;
	HnswBucketData* metap;
	BlockNumber blkno;
	//GenericXLogState *state;
	
	metap = HnswPageGetBucket(metapage);
	pageopaque = (HnswPageOpaque) PageGetSpecialPointer(metapage);

	/* lock overflow buf*/
	ovflbuf = HnswNewBuffer(index);	

	if (isBuild) {
		*ovflpage = BufferGetPage(ovflbuf);
	} else {
		//state = GenericXLogStart(index);
		*ovflpage = GenericXLogRegisterBuffer(state, ovflbuf, GENERIC_XLOG_FULL_IMAGE);
	}

	_hnsw_initbuf(*ovflpage, HNSW_OVERFLOW, metap->level, true);
	
	metap->pages += 1;

	/*
	 * Adjust hashm_firstfree to avoid redundant searches.  But don't risk
	 * changing it if someone moved it while we were searching bitmap pages.
	 */
	blkno = BufferGetBlockNumber(ovflbuf);
	
	metap->first_free = blkno;
	
	/* initialize new overflow page */
	//ovflpage = BufferGetPage(ovflbuf);
	ovflopaque = (HnswPageOpaque) PageGetSpecialPointer(*ovflpage);
	ovflopaque->hnsw_prevblkno = mblk;
	ovflopaque->hnsw_nextblkno = InvalidBlockNumber;
	ovflopaque->level = metap->level;
	ovflopaque->flags = HNSW_OVERFLOW;
	ovflopaque->hnsw_page_id = HNSW_PAGE_ID;

	/* logically chain overflow page to previous page */
	pageopaque->hnsw_nextblkno = blkno;

	if (isBuild) {
		MarkBufferDirty(ovflbuf);
		//MarkBufferDirty(metabuf);
	} else {
		//GenericXLogFinish(state);
	}

	return ovflbuf;
}

#if 0
/*
  通过桶页锁排他
*/

BlockNumber
_freeovflpage(Relation index, Buffer bucketbuf, Buffer ovflbuf, uint16 nitups)
{
	HnswBucketData* metap;
	BlockNumber ovflblkno;
	BlockNumber prevblkno;
	//BlockNumber blkno;
	BlockNumber nextblkno;
	HnswPageOpaque ovflopaque;
	Page		ovflpage;

	Buffer		prevbuf = InvalidBuffer;
	Buffer		nextbuf = InvalidBuffer;
	int16 		level;

	/* Get information from the doomed page */
	ovflblkno = BufferGetBlockNumber(ovflbuf);
	ovflpage = BufferGetPage(ovflbuf);
	ovflopaque = (HnswPageOpaque) PageGetSpecialPointer(ovflpage);
	nextblkno = ovflopaque->hnsw_nextblkno;
	prevblkno = ovflopaque->hnsw_prevblkno;
    level = ovflopaque->level;

	/*
	 * Fix up the bucket chain.  this is a doubly-linked list, so we must fix
	 * up the bucket chain members behind and ahead of the overflow page being
	 * deleted.  Concurrency issues are avoided by using lock chaining as
	 * described atop hashbucketcleanup.
	 */
	if (BlockNumberIsValid(prevblkno)) {
		prevbuf = ReadBuffer(index, prevblkno);
		LockBuffer(prevbuf, BUFFER_LOCK_EXCLUSIVE);
	}
		

	if (BlockNumberIsValid(nextblkno)) {
		nextbuf = ReadBuffer(index, nextblkno);
		LockBuffer(nextbuf, BUFFER_LOCK_EXCLUSIVE);
	}

	LockBuffer(bucketbuf, BUFFER_LOCK_EXCLUSIVE);
	metap = HnswPageGetBucket(BufferGetPage(bucketbuf));

	
	/*
	 * Reinitialize the freed overflow page.  Just zeroing the page won't
	 * work, because WAL replay routines expect pages to be initialized. See
	 * explanation of RBM_NORMAL mode atop XLogReadBufferExtended.  We are
	 * careful to make the special space valid here so that tools like
	 * pageinspect won't get confused.
	 */
	_hnsw_initbuf(ovflpage, BufferGetPageSize(ovflbuf), INVALID_LEVEL, true);

	MarkBufferDirty(ovflbuf);

	if (BufferIsValid(prevbuf))
	{
		Page		prevpage = BufferGetPage(prevbuf);
		HnswPageOpaque prevopaque = (HnswPageOpaque) PageGetSpecialPointer(prevpage);

		Assert(prevopaque->level == level);
		prevopaque->hnsw_nextblkno = nextblkno;
		MarkBufferDirty(prevbuf);
	}
	if (BufferIsValid(nextbuf))
	{
		Page		nextpage = BufferGetPage(nextbuf);
		HnswPageOpaque nextopaque = (HnswPageOpaque) PageGetSpecialPointer(nextpage);

		Assert(nextopaque->level == level);
		nextopaque->hnsw_prevblkno = prevblkno;
		MarkBufferDirty(nextbuf);
	}

	metap->pages -= 1;
	metap->ntuples -= nitups;
	/* if this is now the first free page, update first_free */
	if (ovflblkno == metap->first_free)
	{
		metap->first_free = InvalidBuffer;
	}

	MarkBufferDirty(bucketbuf);
	LockBuffer(bucketbuf, BUFFER_LOCK_UNLOCK);

	if (BufferIsValid(prevbuf))
		UnlockReleaseBuffer(prevbuf);

	if (BufferIsValid(ovflbuf))
		UnlockReleaseBuffer(ovflbuf);

	if (BufferIsValid(nextbuf))
		UnlockReleaseBuffer(nextbuf);

	

	return nextblkno;
}
#endif



ItemPointerData
_greedy_search(HnswState* state, size_t ntuples, void* q, ItemPointer ep)
{
	Relation rel = state->index;
	dist_func func = select_distfunc(state->nproc);
	HnswTuple* entry = _getHnswTuple(state, ep);
	HnswTuple* next_entry;
	float result_distance = func(q, entry->x, entry->dims);
	result_distance = _compute_distance_custom(state, result_distance, entry->bias);
	size_t hops = 0;
	ItemPointerData currobj;
	ItemPointerCopy(ep, &currobj);
	// upper limit 
	for (; hops < ntuples; hops++) {
		//would add ref of buffer
		HnswTuple* result = _getHnswTuple(state, &currobj);
		SpinLockAcquire(&result->mutex);
		bool made_hop = false;
		HnswNode* nodes = HnswGetTupleNodes(result);
		if (result->out_degree > 0) {
			for (size_t i = 0; i < result->out_degree; i++) {
				HnswTuple* nb = _getHnswTuple(state, &(nodes[i].pointer));
				float neighbor_distance = func(q, nb->x, nb->dims);
				neighbor_distance = _compute_distance_custom(state, neighbor_distance, nb->bias);
				if (neighbor_distance < result_distance) {
					result_distance = neighbor_distance;
                    currobj = nodes[i].pointer;
                    made_hop = true;
				}

				_hnsw_dropbuf(rel, nb->buf);

			}
		}
		SpinLockRelease(&result->mutex);
		_hnsw_dropbuf(rel, result->buf);
		if (!made_hop) {
            break;
        }

	}

	// set next level entry
	if (ItemPointerEquals(ep, &currobj)) {
		ItemPointerCopy(&entry->next, &currobj); 
	} else {
		next_entry = _getHnswTuple(state, &currobj);
		ItemPointerCopy(&next_entry->next, &currobj);
		_hnsw_dropbuf(rel, next_entry->buf);
	}

	_hnsw_dropbuf(rel, entry->buf);
	return currobj;
}

/*
 * Comparator for a Min-Heap over the per-tablespace checkpoint completion
 * progress.
 */
static int
hnsw_minheap_comparator(Datum a, Datum b, void *arg)
{
	HnswNode *sa = (HnswNode *) DatumGetPointer(a);
	HnswNode *sb = (HnswNode *) DatumGetPointer(b);
	
	/* we want a min-heap, so return 1 for the a < b */
	if (sa->distance < sb->distance)
		return 1;
	else if (sa->distance == sb->distance)
		return 0;
	else
		return -1;
}

static int
hnsw_maxheap_comparator(Datum a, Datum b, void *arg)
{
	HnswNode *sa = (HnswNode *) DatumGetPointer(a);
	HnswNode *sb = (HnswNode *) DatumGetPointer(b);
	
	/* we want a min-heap, so return 1 for the a < b */
	if (sa->distance < sb->distance)
		return -1;
	else if (sa->distance == sb->distance)
		return 0;
	else
		return 1;
}

int pg_array_cmp(const void *a , const void *b)
{

	HnswNode *sa = (HnswNode *) (a);
	HnswNode *sb = (HnswNode *) (b);
	
	return (sb->distance - sa->distance);
}


/*candidates must max heap*/
pg_array_t*
_select_neighbors_simple(binaryheap* candidates, size_t M)
{

	pg_array_t* result = pg_array_create(M, sizeof(HnswNode));
	// List	   *output = NIL;
	// ListCell   *cell;
	//TODO bug binaryheap_remove_first
	while (!binaryheap_empty(candidates)) {
		HnswNode* node = (HnswNode *) DatumGetPointer(binaryheap_first(candidates));
		binaryheap_remove_first(candidates);
		// pop max distance element
		if (candidates->bh_size >= M) {
			continue;
		}
		HnswNode* ptr = pg_array_push(result);
		*ptr = *node;
		//output = lappend(output, node);

	}
	return result;
}

// TODO Fixme

pg_array_t*
_select_neighbors_heuristic(HnswState* state, binaryheap* candidates, size_t M, int dims)
{
	Relation rel = state->index;
	dist_func func = select_distfunc(state->nproc);
	pg_array_t* output, *reject;

	if (candidates->bh_size <= M) {
		output = _select_neighbors_simple(candidates, M);
		return output;
	}

	output = pg_array_create(M, sizeof(HnswNode));
	reject = pg_array_create(candidates->bh_size, sizeof(HnswNode));
	pairingheap* wset = pairingheap_allocate(pairingheap_HnswSearchItem_cmp, NULL);
	/* convert to minheap */
	while (!binaryheap_empty(candidates)) {
		HnswNode* node = (HnswNode *) DatumGetPointer(binaryheap_first(candidates));
		binaryheap_remove_first(candidates);
		HnswSearchItem* pitem = hnswAllocSearchItem(&node->pointer, node->distance);

		pairingheap_add(wset, &pitem->phNode);
		
	}
	while (!pairingheap_is_empty(wset)) {
		HnswSearchItem* pitem = (HnswSearchItem*) pairingheap_first(wset);
		HnswNode* node = pitem->value;
		float dist_v1_q = node->distance;

		HnswTuple* nn = _getHnswTuple(state, &node->pointer);

		pairingheap_remove_first(wset);
		bool good = true;
		HnswNode *ptrs = (HnswNode*)(output->elts);
		for(int i = 0; i < output->nelts; ++i) {
			HnswNode   *cur =  &ptrs[i];
			HnswTuple* it = _getHnswTuple(state, &cur->pointer);
			float dist_v1_v2 = func(nn->x, it->x, dims);
			_hnsw_dropbuf(rel, it->buf);

			if (dist_v1_v2 < dist_v1_q) {
				good = false;
				break;
			}
		}
		_hnsw_dropbuf(rel, nn->buf);
		if (good) {
			//output = lappend(output, node);
			HnswNode* ptr = pg_array_push(output);
			//print("append", i++, &node->pointer);
			*ptr = *node;
			if (output->nelts >= M) 
				break;
		} else {
			HnswNode* obj = pg_array_push(reject);
			*obj = *node;
		}
	}

	// if (output->nelts < M) {
	// 	int toadd = M - output->nelts;
	// 	HnswNode *nodes = (HnswNode*)(reject->elts);
	// 	for(int j = 0; j < reject->nelts && j < toadd; ++j) {
	// 		HnswNode* ptr = pg_array_push(output);
	// 		*ptr = nodes[j];
	// 	}
	// }
	//elog(INFO, "reject size %u", reject->nelts);
	pg_array_destroy(reject);
	return output;

}

pg_array_t*
_select_neighbors_heuristic2(HnswState* state, HnswNode* nodes, size_t M, HnswNode* add)
{
	Relation rel = state->index;
	dist_func func = select_distfunc(state->nproc);
	int i = 0;
	pg_array_t* output, *reject;

	output = pg_array_create(M, sizeof(HnswNode));
	reject = pg_array_create(M+1, sizeof(HnswNode));
	pairingheap* wset = pairingheap_allocate(pairingheap_HnswSearchItem_cmp, NULL);

	HnswSearchItem* pitem = hnswAllocSearchItem(&add->pointer, add->distance);
	pairingheap_add(wset, &pitem->phNode);	
	/* convert to minheap */
	for (i = 0; i < M ; i++) {
		HnswNode* node = &nodes[i];
		pitem = hnswAllocSearchItem(&node->pointer, node->distance);
		pairingheap_add(wset, &pitem->phNode);	
	}

	while (!pairingheap_is_empty(wset)) {
		HnswSearchItem* pitem = (HnswSearchItem*) pairingheap_first(wset);
		HnswNode* node = pitem->value;
		float dist_v1_q = node->distance;

		HnswTuple* nn = _getHnswTuple(state, &node->pointer);

		pairingheap_remove_first(wset);
		bool good = true;
		
		HnswNode *ptrs = (HnswNode*)(output->elts);
		for(int i = 0; i < output->nelts; ++i) {
			HnswNode   *cur =  &ptrs[i];
			HnswTuple* it = _getHnswTuple(state, &cur->pointer);
			float dist_v1_v2 = func(nn->x, it->x, nn->dims);
			_hnsw_dropbuf(rel, it->buf);

			if (dist_v1_v2 < dist_v1_q) {
				good = false;
				break;
			}
		}
		_hnsw_dropbuf(rel, nn->buf);
		if (good) {
			//output = lappend(output, node);
			HnswNode* ptr = pg_array_push(output);
			*ptr = *node;
			if (output->nelts >= M) 
				break;
		} else {
			HnswNode* obj = pg_array_push(reject);
			*obj = *node;
		}

	}
	// if (output->nelts < M) {
	// 	int toadd = M - output->nelts;
	// 	HnswNode *nodes = (HnswNode*)(reject->elts);
	// 	for(int j = 0; j < reject->nelts && j < toadd; ++j) {
	// 		HnswNode* ptr = pg_array_push(output);
	// 		*ptr = nodes[j];
	// 	}
	// }
	pg_array_destroy(reject);
	return output;

}

void make_link(HnswState* state, HnswNode* current, HnswNode* target, bool isBuild)
{	
	if (ItemPointerEquals(&current->pointer, &target->pointer)) {
		//elog(INFO, "link self.");
		return;
	}
	Relation rel = state->index;
	dist_func func = select_distfunc(state->nproc);
	HnswTuple* source = _getHnswTuple(state, &current->pointer);
	Assert(ItemPointerEquals(&current->pointer, &source->iptr));
	Buffer buf = source->buf;
	SpinLockAcquire(&source->mutex);
	size_t maxM = source->maxM;
	// update distance
	HnswTuple* neighbor = _getHnswTuple(state, &target->pointer);
	target->distance = func(source->x, neighbor->x, source->dims);
	neighbor->in_degree++;
	_hnsw_dropbuf(rel, neighbor->buf);

	HnswNode* nodes = HnswGetTupleNodes(source);
	if (source->out_degree < maxM) {
		nodes[source->out_degree] = *target;
		source->out_degree++;
	} else {
		Assert(source->out_degree == maxM);
		pg_array_t* result = _select_neighbors_heuristic2(state, nodes, maxM, target);

		// 距离降序
		qsort(result->elts, result->nelts ,sizeof(HnswNode), pg_array_cmp);

		HnswNode *ptr = (HnswNode*)(result->elts);
		for(int j = 0; j < result->nelts; ++j) {
			nodes[j] = ptr[j];
		}

		source->out_degree = result->nelts;
		pg_array_destroy(result);
		Assert(source->out_degree <= maxM);
	}

	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	OffsetNumber offnum = ItemPointerGetOffsetNumber(&source->iptr);
	//overwrite and update links
	Assert(offnum != 0);
	PageIndexTupleOverwrite(BufferGetPage(buf), offnum, (Item)source, source->size_tuple);
	MarkBufferDirty(buf);
	LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	
	SpinLockRelease(&source->mutex);
	_hnsw_dropbuf(rel, buf);
}

void
bidirection_connect(HnswState* state, binaryheap* candidates, HnswTuple* cur, bool isBuild)
{
	Relation rel = state->index;
	size_t maxM = cur->maxM;
	dist_func func = select_distfunc(state->nproc);
	pg_array_t* output;
	int  idx, i = 0;
	output = _select_neighbors_heuristic(state, candidates, maxM, cur->dims);

	qsort(output->elts, output->nelts ,sizeof(HnswNode), pg_array_cmp);

	HnswNode *ptr = (HnswNode*)(output->elts);

	HnswNode* nodes = HnswGetTupleNodes(cur);
	for(i = 0; i < output->nelts; ++i) {
		nodes[i]= ptr[i];
	}
	//memcpy(nodes, output->elts, output->size * output->nelts);
	idx = output->nelts;
	cur->out_degree = idx;
	Assert(idx <= maxM);
	// update neighbor
	for (i = 0; i < idx; i++ ) {
		//print("neighbor ",i, &nodes[i].pointer);
		HnswTuple* neighbor = _getHnswTuple(state, &nodes[i].pointer);
		neighbor->in_degree++;
		if (neighbor->out_degree > maxM) {
			elog(ERROR, "Bad value of out_degree");
		}
		if (ItemPointerEquals(&nodes[i].pointer, &cur->iptr)) {
			elog(ERROR, "Trying to connect an element to itself");
		}
		if (cur->level != neighbor->level) {
			elog(ERROR,"Trying to make a link on a non-existent level");
		}
		HnswNode* neighbor_nodes = HnswGetTupleNodes(neighbor);
		float dist = func(cur->x, neighbor->x, cur->dims);
		if (neighbor->out_degree < maxM) {
			
			neighbor_nodes[neighbor->out_degree].pointer = cur->iptr;
			neighbor_nodes[neighbor->out_degree].distance = dist;
			neighbor->out_degree++;
			cur->in_degree++;
			
		} else {
		
			HnswNode* newnode = (HnswNode*) palloc0(sizeof(HnswNode));
			newnode->distance = dist;
			newnode->pointer = cur->iptr;
			
			// replace weakest element
			Assert(neighbor->out_degree == maxM);
			pg_array_t* result = _select_neighbors_heuristic2(state, neighbor_nodes, maxM, newnode);

			qsort(result->elts, result->nelts ,sizeof(HnswNode), pg_array_cmp);

			HnswNode *ptr = (HnswNode*)(result->elts);
			for(int j = 0; j < result->nelts; ++j) {
				HnswNode   *curnode =  &ptr[j];
				if (ItemPointerEquals(&cur->iptr, &curnode->pointer))
					cur->in_degree++;
				neighbor_nodes[j] = *curnode;
			}

			neighbor->out_degree = result->nelts;
			pg_array_destroy(result);
			Assert(neighbor->out_degree <= maxM);

		}
		
		_updateHnswTuple(rel, &nodes[i].pointer, neighbor, isBuild);
		_hnsw_dropbuf(rel, neighbor->buf);
	}
	pg_array_destroy(output);
	_updateHnswTuple(rel, &cur->iptr, cur, isBuild);

}

// TODO opt code
void
bidirection_connect2(HnswState* state, binaryheap* candidates, ItemPointerData iptr, int lv, bool isBuild)
{

	size_t maxM = (lv == 0) ? 2*state->max_links: state->max_links;
	
	pg_array_t* output;
	int  i = 0;
	output = _select_neighbors_heuristic(state, candidates, maxM, state->dims);

	qsort(output->elts, output->nelts ,sizeof(HnswNode), pg_array_cmp);	

	HnswNode *ptr = (HnswNode*)(output->elts);
	HnswNode current = {iptr, 0};
	for(i = 0; i < output->nelts; ++i) {
		//print("neighbor ", i, &ptr[i].pointer);
		make_link(state, &current, &ptr[i], isBuild);

		make_link(state, &ptr[i], &current, isBuild);
	}
	pg_array_destroy(output);
	
}


void
bidirection_connect_simple(HnswState* state, binaryheap* candidates, HnswTuple* cur, bool isBuild)
{
	// cur node hold q
	Relation rel = state->index;
	size_t maxM = cur->maxM;
	dist_func func = select_distfunc(state->nproc);
	pg_array_t* output;
	int idx = 0, i = 0;
	output = _select_neighbors_simple(candidates, maxM);
	HnswNode* nodes = (HnswNode*) ((char*)cur+cur->offset_out_links);

	HnswNode *ptr = (HnswNode*)(output->elts);
	for(i = 0; i < output->nelts; ++i) {
		nodes[i]= ptr[i];
	}
	//memcpy(nodes, output->elts, output->size * output->nelts);
	idx = output->nelts;
	cur->out_degree = idx;
	Assert(idx <= maxM);
	// update neighbor
	for (i = 0; i < idx; i++ ) {
		//print("neighbor ",i, &nodes[i].pointer);
		HnswTuple* neighbor = _getHnswTuple(state, &nodes[i].pointer);
		neighbor->in_degree++;
		if (neighbor->out_degree > maxM) {
			elog(ERROR, "Bad value of out_degree");
		}
		if (ItemPointerEquals(&nodes[i].pointer, &cur->iptr)) {
			elog(ERROR, "Trying to connect an element to itself");
		}
		if (cur->level != neighbor->level) {
			elog(ERROR,"Trying to make a link on a non-existent level");
		}
		HnswNode* neighbor_nodes = HnswGetTupleNodes(neighbor);
		float dist = func(cur->x, neighbor->x, cur->dims);
		dist = _compute_distance_custom(state, dist, neighbor->bias);
		if (neighbor->out_degree < maxM) {
			neighbor_nodes[neighbor->out_degree].pointer = cur->iptr;
			neighbor_nodes[neighbor->out_degree].distance = dist;
			neighbor->out_degree++;
			cur->in_degree++;
		} else {
			float maxdist = dist;
			int indx = -1;
			for (int j = 0 ; j < neighbor->out_degree; j++) {
				if (neighbor_nodes[j].distance > maxdist) {
					maxdist = neighbor_nodes[j].distance;
					indx = j;
				}
			}
			//replace max distance, TODO fix (in_degree) = 0 case
			if (indx >= 0) { 
			#ifdef DEBUG
				HnswTuple* nn = _getHnswTuple(state, &neighbor_nodes[indx].pointer);
				if (nn->in_degree == 1) {
					elog(INFO, "Bad value of in_degree");
				}
				nn->in_degree--;
				_hnsw_dropbuf(rel, nn->buf);

				_updateHnswTuple(rel, &neighbor_nodes[indx].pointer, nn, isBuild);
			#endif
				neighbor_nodes[indx].distance = dist;
				neighbor_nodes[indx].pointer = cur->iptr;
				cur->in_degree++;
			}

			neighbor->out_degree = maxM;
			Assert(neighbor->out_degree == maxM);
		
		}
		_updateHnswTuple(rel, &nodes[i].pointer, neighbor, isBuild);
		_hnsw_dropbuf(rel, neighbor->buf);

	}
	pg_array_destroy(output);
	_updateHnswTuple(rel, &cur->iptr, cur, isBuild);
}


binaryheap*
_search_level(HnswState* state,
                  size_t ef,
                  size_t level,
                  void* q, ItemPointer ep)
{

	ItemPointerSet intset;
	float lowerBound, dist;
	HnswTuple *cur;
	HnswNode* node;
	HnswSearchItem* pitem;
	ItemPointerData nlipt;
	Relation rel = state->index;
	dist_func func = select_distfunc(state->nproc);
	intset = stlset_create();
	// top_candidates is maxheap
	binaryheap* top_candidates = binaryheap_allocate(ef+1,
								  hnsw_maxheap_comparator, NULL);
	// candidate_set is minheap
	pairingheap* candidate_set = pairingheap_allocate(pairingheap_HnswSearchItem_cmp, NULL);
	
	HnswTuple* entry = _getHnswTuple(state, ep);
	dist = func(q, entry->x, entry->dims);
	dist = _compute_distance_custom(state, dist, entry->bias);
	nlipt = entry->next;
	_hnsw_dropbuf(rel, entry->buf);

	lowerBound = dist;

	pitem = hnswAllocSearchItem(ep, dist);

	binaryheap_add(top_candidates, PointerGetDatum(pitem->value));

	pairingheap_add(candidate_set, &pitem->phNode);

	// set visited
	stlset_add_member(&intset, itemptr_encode(ep));

	while (!pairingheap_is_empty(candidate_set)) {
		pitem = (HnswSearchItem*) pairingheap_first(candidate_set);
		node = pitem->value; 
		if (node->distance > lowerBound) {
			break;
		}
		pairingheap_remove_first(candidate_set);

		cur = _getHnswTuple(state, &node->pointer);
		SpinLockAcquire(&cur->mutex);
		HnswNode* nodes = HnswGetTupleNodes(cur);
		for (size_t i = 0; i < cur->out_degree; i++) {
			ItemPointer pt = &nodes[i].pointer;
			if (stlset_is_member(&intset, itemptr_encode(pt))) {
				continue;
			}
			stlset_add_member(&intset, itemptr_encode(pt));
			HnswTuple* ni = _getHnswTuple(state, pt);
			float dist1 = func(q, ni->x, ni->dims);
			dist1 = _compute_distance_custom(state, dist1, ni->bias);
			bool deleted = ni->deleted;
			_hnsw_dropbuf(rel, ni->buf);

			if (top_candidates->bh_size < ef || dist1 < lowerBound) {
				HnswSearchItem* item = hnswAllocSearchItem(pt, dist1);
				pairingheap_add(candidate_set, &item->phNode);
				if (!deleted)
					binaryheap_add(top_candidates, PointerGetDatum(item->value));
				//remove max distance element
				if (top_candidates->bh_size > ef) {
					binaryheap_remove_first(top_candidates);
				}
				if (!binaryheap_empty(top_candidates)) {
					node = (HnswNode *) DatumGetPointer(binaryheap_first(top_candidates));
					lowerBound = node->distance;
				}
			}
		}
		SpinLockRelease(&cur->mutex);
		_hnsw_dropbuf(rel, cur->buf);
		hnswFreeSearchItem(pitem);
		
	}
	pairingheap_free(candidate_set);
	stlset_release(&intset);
	*ep = nlipt;
	return top_candidates;
}




/*
	hold buf ref count
*/

HnswTuple* 
_getHnswTuple(HnswState* s, ItemPointer ptr)
{
	Buffer buf;
	Page page;
	OffsetNumber offnum;
	HnswTuple* itup;
	Relation rel = s->index;

	buf = ReadBuffer(rel, ItemPointerGetBlockNumber(ptr));
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	offnum = ItemPointerGetOffsetNumber(ptr);
	Assert(offnum != 0);
	itup = (HnswTuple*) PageGetItem(page,
										PageGetItemId(page, offnum));
	
	itup->buf = buf;
	LockBuffer(buf, BUFFER_LOCK_UNLOCK);
// #ifdef DEBUG
// 	HnswMetaPage metap = (HnswMetaPage)rel->rd_amcache;
// 	metap->search_count++;
// #endif
	return itup;
}


/*
	get tuple copy
*/

storageType*
_getTupleArray(Relation rel, BlockNumber blk, OffsetNumber offnum)
{
	Buffer buf;
	Page page;
	
	HnswTuple* itup;
	storageType* q;
	buf = ReadBuffer(rel, blk);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	Assert(offnum != 0);
	itup = (HnswTuple*) PageGetItem(page,
										PageGetItemId(page, offnum));
	
	q = (storageType*) palloc0(sizeof(storageType) * itup->dims);
	memcpy(q, itup->x, sizeof(storageType) * itup->dims);
	UnlockReleaseBuffer(buf);

	return q;
}



bool
_updateHnswTuple(Relation index, ItemPointer ptr, HnswTuple* tuple, bool isBuild)
{
	GenericXLogState *state;
	Page page;
	//
	Buffer buf = _hnsw_getbuf(index, ItemPointerGetBlockNumber(ptr), BUFFER_LOCK_EXCLUSIVE);
	if (isBuild) {
		page = BufferGetPage(buf);
	} else {
		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, 0);
	}

	OffsetNumber offnum = ItemPointerGetOffsetNumber(ptr);
	//overwrite itup
	Assert(offnum != 0);
	PageIndexTupleOverwrite(page, offnum, (Item)tuple, tuple->size_tuple);

	if (isBuild) {
		MarkBufferDirty(buf);
	} else {
		GenericXLogFinish(state);
	}
	_hnsw_relbuf(index, buf);
	return true;
}



float _inner_dot(const float * x, const float * y, size_t d) {
	return 1.0f - fvec_inner_product(x, y, d);
}

dist_func select_distfunc(int nproc) {
	switch (nproc) {
		case L2_DIST: return fvec_L2sqr;
		case DOT_DIST: return _inner_dot;
		case CUSTOM_DOT_DIST: return fvec_inner_product;
		default:
			elog(FATAL, "unknown algorithm.");
	}
	return NULL;
	
}

float _compute_distance_custom(HnswState* s, float d, float bias) {

	float dist = d;
	if (s->nproc == CUSTOM_DOT_DIST) {
		Assert(s->atrrnum >= 1);
		dist = DatumGetFloat8(FunctionCall2Coll(&s->distanceFn[s->atrrnum - 1], s->collations[s->atrrnum - 1], \
							   Float4GetDatum(d), Float4GetDatum(bias)));
	}
	
	return dist;
}

