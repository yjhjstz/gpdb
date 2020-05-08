/*-------------------------------------------------------------------------
 *
 * hninsert.c
 *		Hnsw index build and insert functions.
 *
 * Copyright (c) 2016-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/quantum/hninsert.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/generic_xlog.h"
#if PG_VERSION_NUM >= 120000
#include "access/tableam.h"
#endif
#include "access/parallel.h"
#include <access/xact.h>

#include "pgstat.h"
#include "commands/progress.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "storage/smgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "hnsw.h"


#if PG_VERSION_NUM >= 120000
#define IndexBuildHeapScan(A, B, C, D, E, F) \
table_index_build_scan(A, B, C, D, true, E, F, NULL)
#elif PG_VERSION_NUM >= 110000
#define IndexBuildHeapScan(A, B, C, D, E, F) \
IndexBuildHeapScan(A, B, C, D, E, F, NULL)
#endif

/* GUC parameter */
int			index_parallel = 0;
bool 		link_nearest = false;
/*
 * State of hnsw index build.  We accumulate one page data here before
 * flushing it to buffer manager.
 */
typedef struct
{
	HnswState	blstate;		/* hnsw index state */
	int64		indtuples;		/* total number of tuples indexed */
	MemoryContext tmpCtx;		/* temporary memory context reset after each
								 * tuple */
	bool		flush;			/* flush meta page */
	int			count;			/* number of tuples in cached page */
	int 		maxlevel;
} HnswBuildState;


typedef struct
{
	Oid indexrelid;
	ItemPointerData L1start;
	size_t L1ntuples;
	size_t L0pages;
	int nprocess;

	slock_t mutex;
	int nworker;
	int blkno;
	size_t done_pages;

} HnswShared;


void 
_build_L0index(dsm_segment *seg, shm_toc *toc)
{
	volatile HnswShared *pshared = shm_toc_lookup(toc, 1);
	Relation index = index_open(pshared->indexrelid, ShareLock);
	MemoryContext oldCtx, tmpCtx;
	size_t L1ntuples = pshared->L1ntuples;
    size_t L0pages = pshared->L0pages;

	ItemPointerData start = pshared->L1start;
	
	HnswState state;
	//update index
	// state.index = index;
	initHnswState(&state, index);
	state.isBuild = true;

	SpinLockAcquire(&pshared->mutex);
	int nworker = pshared->nworker++;
    SpinLockRelease(&pshared->mutex);

    elog(INFO, "build in worker #%u", nworker);
    tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
								   "Hnsw worker temporary context",
									ALLOCSET_LARGE_SIZES);

    pgstat_progress_start_command(PROGRESS_COMMAND_CREATE_INDEX,
									  pshared->indexrelid);
    pgstat_progress_update_param(PROGRESS_CREATEIDX_PARTITIONS_TOTAL,
											 L0pages);

	while (true) {

		CHECK_FOR_INTERRUPTS();

		Buffer		buffer;
		Page		page;
		int i, max, nextblk, percent;
		size_t done = 0;
		HnswPageOpaque pageopaque;
		ItemPointerData entry, iptr;
		
		SpinLockAcquire(&pshared->mutex);

		volatile int blkno = pshared->blkno;
		
		if(blkno == InvalidBlockNumber) {
			SpinLockRelease(&pshared->mutex);
            break;
		}

		buffer = ReadBuffer(index, blkno);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buffer);
		Assert(HnswPageIsOverflow(page));
		pageopaque = (HnswPageOpaque) PageGetSpecialPointer(page);
		nextblk = pageopaque->hnsw_nextblkno;
		max = PageGetMaxOffsetNumber(page);
		UnlockReleaseBuffer(buffer);

		// set net blkno
		pshared->blkno = nextblk;

		done = pshared->done_pages;
		
		pshared->done_pages += 1;

        SpinLockRelease(&pshared->mutex);
        
        oldCtx = MemoryContextSwitchTo(tmpCtx);

        percent = 100 * done / L0pages;
        
        if (done % 100 == 0) {
            pgstat_progress_update_param(PROGRESS_CREATEIDX_PARTITIONS_DONE, done);
            //elog(INFO, "worker#%d, building progress %d%%", nworker, percent);
        }
        // COW
		for (i = FirstOffsetNumber; i <= max; i++) {
			entry = start;
			storageType* q = _getTupleArray(index, blkno, i);

			ItemPointerSet(&iptr, blkno, i);

			entry = _greedy_search(&state, L1ntuples, q, &entry);

			binaryheap* candidates = _search_level(&state, state.ef_construction, 0, q, &entry);
			// set links
			bidirection_connect2(&state, candidates, iptr, 0, true);
		
			binaryheap_free(candidates);
			pfree(q);
		}

		MemoryContextSwitchTo(oldCtx);
		MemoryContextReset(tmpCtx);
	}


	index_close(index, ShareLock);
	pgstat_progress_end_command();
	MemoryContextDelete(tmpCtx);
	//elog(INFO, "build L0 worker# %u leave.", nworker);
}

/*
 * Flush page cached in BloomBuildState.
 */
static void
flushCachedPage(Relation index, HnswBuildState *buildstate)
{
	elog(INFO, "build index indtuples %ld, count %d.", 
				buildstate->indtuples, buildstate->count);

	HnswUpdateMetapage(index, buildstate->maxlevel, buildstate->blstate.isBuild);
}

/*
 * (Re)initialize cached page in BloomBuildState.
 */
static void
initCachedPage(HnswBuildState *buildstate)
{
	buildstate->flush = false;
	buildstate->count = 0;
	buildstate->indtuples = 0;
	buildstate->maxlevel = INVALID_LEVEL;
}


void _hnsw_insert(Relation index, Datum *values, bool *isnull,
		 ItemPointer ht_ctid, void* state, bool refresh)
{
	HnswTuple *itup;
	HnswTuple **itups;
	HnswMetaPage metap;
	MemoryContext oldCtx;
	OffsetNumber offnum;
	HnswBucketData* bucketp;
	int l, lc;

	Buffer bucketbuf;
	Buffer	metabuf = InvalidBuffer;
	ItemPointer ipd = NULL;
	ItemPointerData start;
	GenericXLogState *xstate = NULL;
	float* dur;
	HnswBuildState *buildstate = (HnswBuildState *) state;
	Size		itemsz = buildstate->blstate.sizeOfHnswTuple;
	bool isBuild = buildstate->blstate.isBuild;
	int level = random_level(buildstate->blstate.max_links) % MAX_LEVEL;
	Assert(level < MAX_LEVEL && level >= 0);
	Assert(buildstate->blstate.atrrnum >= 1);
	//NDBOX* arr = DatumGetNDBOXP(values[buildstate->blstate.atrrnum - 1]);
	ArrayType* arr = DatumGetArrayTypeP(values[buildstate->blstate.atrrnum - 1]);
	storageType* q = (storageType*) palloc(sizeof(storageType) * ARRNELEMS(arr));
	dur = ARRPTR(arr);

	for (int i = 0; i < ARRNELEMS(arr); i++) {
		q[i] = dur[i];
	}
	oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

	metap = _getcachedmetap(index, &metabuf, refresh);
	Assert(metap);

	lc = metap->max_level_;

	if (metap->max_level_ < level) {
		metap->max_level_ = level;
		buildstate->flush = true;
	}
	buildstate->maxlevel = metap->max_level_;

	// handle level 0
	ipd = (ItemPointer) palloc0(sizeof(ItemPointerData) * (level +1));
	itups = (HnswTuple**) palloc0(sizeof(void*) * (level+1));
	//elog(INFO, "blinsert begin level %d %d", level, lc);
	ItemPointerSetInvalid(&start);
	if (lc != INVALID_LEVEL) {
		// search layers
		for (l = lc; l >= level+1; l--) {
			bucketbuf = _hnsw_getbuf(index, metap->level_blk[l], BUFFER_LOCK_SHARE);
			bucketp = HnswPageGetBucket(BufferGetPage(bucketbuf));
			if (!ItemPointerIsValid(&start)) {
				start = bucketp->entry;
			}
			Assert(ItemPointerIsValid(&start));
			start = _greedy_search(&buildstate->blstate, bucketp->ntuples, q, &start);
			
			_hnsw_relbuf(index, bucketbuf);
		}

	}


	for (l = level; l >= 0; l--) {
		BlockNumber blk; // bucket page
		Buffer ovflbuf, rbuf = InvalidBuffer;
		Page page, bkpage, ovflpage;
		bool registered = false;

		bucketbuf = _hnsw_getbuf(index, metap->level_blk[l], BUFFER_LOCK_EXCLUSIVE);
		if (isBuild) {
			bkpage = BufferGetPage(bucketbuf);
		} else {
			xstate = GenericXLogStart(index);
			bkpage = GenericXLogRegisterBuffer(xstate, bucketbuf, GENERIC_XLOG_FULL_IMAGE);
		}
		
		bucketp = HnswPageGetBucket(bkpage);
		//elog(INFO , "l: %d, %d.", l, bucketp->level);
		Assert(bucketp->level == l);


		if (bucketp->first_free == InvalidBlockNumber) {
			Assert(bucketp->pages == 0 );
			// lock the ovflbuf
			ovflbuf = _addfirstpage(xstate, index, bkpage, BufferGetBlockNumber(bucketbuf), isBuild, &ovflpage);
			registered = true;
		} else {
			ovflbuf = _hnsw_getbuf(index, bucketp->first_free, BUFFER_LOCK_EXCLUSIVE);
			ovflpage = BufferGetPage(ovflbuf);
		}
		

		itup = HnswFormTuple(&buildstate->blstate, ht_ctid, values, isnull, l);

		if (PageGetFreeSpace(ovflpage) > itemsz) {
			if (!isBuild && !registered) {
				ovflpage = GenericXLogRegisterBuffer(xstate, ovflbuf, 0);
			}
			offnum = _hnsw_pgaddtup(index, ovflpage, itemsz, itup);
			blk = BufferGetBlockNumber(ovflbuf);
		} else {
			LockBuffer(ovflbuf, BUFFER_LOCK_UNLOCK);
			CHECK_FOR_INTERRUPTS();

			rbuf = _addovflpage(xstate, index, bkpage, ovflbuf, false, isBuild);

			if (isBuild) {
				page = BufferGetPage(rbuf);
			} else {
				page = GenericXLogRegisterBuffer(xstate, rbuf, 0);
			}
			
			offnum = _hnsw_pgaddtup(index, page, itemsz, itup);
			blk = BufferGetBlockNumber(rbuf);
			
		}
		
		ItemPointerSet(&itup->iptr, blk , offnum);
		
		// set entry point
		if (bucketp->ntuples++ == 0){
			ItemPointerSet(&bucketp->entry, blk , offnum);
		}
		if (isBuild) {
			MarkBufferDirty(ovflbuf);
			MarkBufferDirty(bucketbuf);
			if (BufferIsValid(rbuf)) {
				MarkBufferDirty(rbuf);
			}
		} else {
			GenericXLogFinish(xstate);
		}
		
		_hnsw_relbuf(index, ovflbuf);
		_hnsw_relbuf(index, bucketbuf);
		if (BufferIsValid(rbuf)) {
			_hnsw_relbuf(index, rbuf);
		}

		if (ItemPointerIsValid(&start)) {
			binaryheap* candidates = _search_level(&buildstate->blstate, 
				buildstate->blstate.ef_construction, l, q, &start);
			// set links
			if (link_nearest) {
				bidirection_connect_simple(&buildstate->blstate, candidates, itup, isBuild);
			} else {
				bidirection_connect(&buildstate->blstate, candidates, itup, isBuild);
			}
			binaryheap_free(candidates);
		}

		buildstate->count++;
				
		ItemPointerSet(&ipd[l], blk , offnum);
		itups[l] = itup;
		
	}

	// link layers and overwrite itup
	for (l = level; l >= 0; l--) {
		itup = itups[l];
		ItemPointerCopy(&ipd[l], &itup->iptr);
		if (l > 0)
			ItemPointerCopy(&ipd[l-1], &itup->next);
		//overwrite itup
		_updateHnswTuple(index, &itup->iptr, itup, isBuild);

	}

	/* Update total tuple count */
	buildstate->indtuples += 1;
	pfree(ipd);
	pfree(itups);
	pfree(q);
	
	// release meta pin
	if (BufferIsValid(metabuf))
		_hnsw_dropbuf(index, metabuf);

	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(buildstate->tmpCtx);
	return;
}


void _hnsw_insert_data(Relation index, Datum *values, bool *isnull,
		 ItemPointer ht_ctid, void* state, bool refresh)
{
	HnswTuple *itup;
	HnswTuple **itups;
	HnswMetaPage metap;
	MemoryContext oldCtx;
	OffsetNumber offnum;
	HnswBucketData* bucketp;
	int l, lc;

	Buffer bucketbuf;
	Buffer	metabuf = InvalidBuffer;
	ItemPointer ipd = NULL;
	GenericXLogState *xstate = NULL;
	float* dur;
	HnswBuildState *buildstate = (HnswBuildState *) state;
	Size		itemsz = buildstate->blstate.sizeOfHnswTuple;
	bool isBuild = buildstate->blstate.isBuild;
	int level = random_level(buildstate->blstate.max_links) % MAX_LEVEL;
	Assert(level < MAX_LEVEL && level >= 0);
	Assert(buildstate->blstate.atrrnum >= 1);
	//NDBOX* arr = DatumGetNDBOXP(values[buildstate->blstate.atrrnum - 1]);
	ArrayType* arr = DatumGetArrayTypeP(values[buildstate->blstate.atrrnum - 1]);
	storageType* q = (storageType*) palloc(sizeof(storageType) * ARRNELEMS(arr));
	dur = ARRPTR(arr);

	for (int i = 0; i < ARRNELEMS(arr); i++) {
		q[i] = dur[i];
	}
	oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

	metap = _getcachedmetap(index, &metabuf, refresh);
	Assert(metap);

	lc = metap->max_level_;

	if (metap->max_level_ < level) {
		metap->max_level_ = level;
		buildstate->flush = true;
	}
	buildstate->maxlevel = metap->max_level_;

	// handle level 0
	ipd = (ItemPointer) palloc0(sizeof(ItemPointerData) * (level +1));
	itups = (HnswTuple**) palloc0(sizeof(void*) * (level+1));

	for (l = level; l >= 0; l--) {
		BlockNumber blk; // bucket page
		Buffer ovflbuf, rbuf = InvalidBuffer;
		Page page, bkpage, ovflpage;

		bucketbuf = _hnsw_getbuf(index, metap->level_blk[l], BUFFER_LOCK_EXCLUSIVE);
		bkpage = BufferGetPage(bucketbuf);
		
		bucketp = HnswPageGetBucket(bkpage);
		//elog(INFO , "l: %d, %d.", l, bucketp->level);
		Assert(bucketp->level == l);


		if (bucketp->first_free == InvalidBlockNumber) {
			Assert(bucketp->pages == 0 );
			// lock the ovflbuf
			ovflbuf = _addfirstpage(xstate, index, bkpage, BufferGetBlockNumber(bucketbuf), isBuild, &ovflpage);
			
		} else {
			ovflbuf = _hnsw_getbuf(index, bucketp->first_free, BUFFER_LOCK_EXCLUSIVE);
			ovflpage = BufferGetPage(ovflbuf);
		}
		

		itup = HnswFormTuple(&buildstate->blstate, ht_ctid, values, isnull, l);
		
		if (PageGetFreeSpace(ovflpage) > itemsz) {
			offnum = _hnsw_pgaddtup(index, ovflpage, itemsz, itup);
			blk = BufferGetBlockNumber(ovflbuf);
		} else {
			LockBuffer(ovflbuf, BUFFER_LOCK_UNLOCK);
			CHECK_FOR_INTERRUPTS();

			rbuf = _addovflpage(xstate, index, bkpage, ovflbuf, false, isBuild);

			page = BufferGetPage(rbuf);
			
			offnum = _hnsw_pgaddtup(index, page, itemsz, itup);
			blk = BufferGetBlockNumber(rbuf);
			
		}
		
		ItemPointerSet(&itup->iptr, blk , offnum);
		
		// set entry point
		if (bucketp->ntuples++ == 0){
			ItemPointerSet(&bucketp->entry, blk , offnum);
		}
		
		MarkBufferDirty(ovflbuf);
		MarkBufferDirty(bucketbuf);
		if (BufferIsValid(rbuf)) {
			MarkBufferDirty(rbuf);
		}
		
		_hnsw_relbuf(index, ovflbuf);
		_hnsw_relbuf(index, bucketbuf);
		if (BufferIsValid(rbuf)) {
			_hnsw_relbuf(index, rbuf);
		}

		buildstate->count++;
				
		ItemPointerSet(&ipd[l], blk , offnum);
		itups[l] = itup;
		
	}

	// link layers and overwrite itup
	for (l = level; l >= 0; l--) {
		itup = itups[l];
		ItemPointerCopy(&ipd[l], &itup->iptr);
		if (l > 0)
			ItemPointerCopy(&ipd[l-1], &itup->next);
		//overwrite itup
		_updateHnswTuple(index, &itup->iptr, itup, isBuild);

	}

	/* Update total tuple count */
	buildstate->indtuples += 1;
	pfree(ipd);
	pfree(itups);
	pfree(q);
	
	// release meta pin
	if (BufferIsValid(metabuf))
		_hnsw_dropbuf(index, metabuf);

	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(buildstate->tmpCtx);
	return;
}

/*
 * Per-tuple callback for table_index_build_scan.
 */
static void
hnswBuildCallback(Relation index, ItemPointer tupleId, Datum *values,
				   bool *isnull, bool tupleIsAlive, void *state)
{

	_hnsw_insert(index, values, isnull, tupleId, state, false);
	//elog(INFO, "insert end");
}


static void
hnswBuildCallback2(Relation index, ItemPointer tupleId, Datum *values,
				   bool *isnull, bool tupleIsAlive, void *state)
{

	_hnsw_insert_data(index, values, isnull, tupleId, state, false);
	//elog(INFO, "insert end");
}

/*
 * Build a new hnsw index.
 */


IndexBuildResult *
blbuild_seq(Relation heap, Relation index, IndexInfo *indexInfo)
{
	//elog(INFO, "blbuild entry");
	IndexBuildResult *result;
	double		reltuples;
	HnswBuildState buildstate;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			 RelationGetRelationName(index));

	/* Initialize the meta page */
	//START_CRIT_SECTION();
	HnswInitMetapage(index, true);
	//END_CRIT_SECTION();
	/* Initialize the bloom build state */
	//memset(&buildstate, 0, sizeof(buildstate));
	initHnswState(&buildstate.blstate, index);
	buildstate.blstate.isBuild = true;
	buildstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											  "Hnsw build temporary context",
											  ALLOCSET_LARGE_SIZES);
	initCachedPage(&buildstate);

	/* Do the heap scan */
	reltuples = IndexBuildScan(heap, index, indexInfo, true,
									   hnswBuildCallback, (void *) &buildstate);

	/* Flush last page if needed (it will be, unless heap was empty) */
	if (buildstate.flush)
		flushCachedPage(index, &buildstate);

	MemoryContextDelete(buildstate.tmpCtx);

	/*
	 * Write index to xlog
	 */
	for (int blkno = 0; blkno < RelationGetNumberOfBlocks(index); blkno++)
	{
		Buffer		buffer;
		GenericXLogState *state;

		CHECK_FOR_INTERRUPTS();

		buffer = _hnsw_getbuf(index, blkno, BUFFER_LOCK_EXCLUSIVE);
		

		state = GenericXLogStart(index);
		GenericXLogRegisterBuffer(state, buffer, GENERIC_XLOG_FULL_IMAGE);
		GenericXLogFinish(state);

		// UnlockReleaseBuffer(buffer);
		_hnsw_relbuf(index, buffer);
	}

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = reltuples;
	result->index_tuples = buildstate.indtuples;

	return result;
}

void build_graph(HnswBuildState* state, Relation index)
{

	int max_level = state->maxlevel, l;
	size_t L1ntuples = 0;
	size_t L0pages = 0;
	BlockNumber blkno, nextblk;
	MemoryContext oldCtx;
	Page metapage;
	HnswBucketData* bucketp;
	HnswMetaPage metap;
	HnswPageOpaque metaopaque, pageopaque;

	ItemPointerData start, entry, iptr, L1entry;
	Buffer bucketbuf;
	Buffer  metabuf = InvalidBuffer;

	//oldCtx = MemoryContextSwitchTo(state->tmpCtx);

  	metap = _getcachedmetap(index, &metabuf, false);
    
	for (l = max_level; l >= 0; l--) {
		elog(INFO, "build level %d.", l);
		bucketbuf = _hnsw_getbuf(index, metap->level_blk[l], BUFFER_LOCK_SHARE);
		metapage = BufferGetPage(bucketbuf);
		bucketp = HnswPageGetBucket(metapage);
		
		start = bucketp->entry;
		L0pages = bucketp->pages;

		if (l == 1u) {
			L1ntuples = bucketp->ntuples;
			L1entry = start;
		}
		
		Assert(ItemPointerIsValid(&start));
		if (bucketp->ntuples == 1) {
			//elog(INFO, "skip build level %u.", l);
			_hnsw_relbuf(index, bucketbuf);
		} else {
			metaopaque = (HnswPageOpaque) PageGetSpecialPointer(metapage);
			blkno = metaopaque->hnsw_nextblkno;

			_hnsw_relbuf(index, bucketbuf);
			
			if (l == 0u && index_parallel > 0) {
				break;
			}

			while (blkno != InvalidBlockNumber) {
				CHECK_FOR_INTERRUPTS();
				//elog(INFO, "build page %u.", blkno);
				Buffer		buffer;
				Page		page;
				int i, max;
				buffer = ReadBuffer(index, blkno);
				LockBuffer(buffer, BUFFER_LOCK_SHARE);

				page = BufferGetPage(buffer);
				Assert(HnswPageIsOverflow(page));
				pageopaque = (HnswPageOpaque) PageGetSpecialPointer(page);
				nextblk = pageopaque->hnsw_nextblkno;
				max = PageGetMaxOffsetNumber(page);
				UnlockReleaseBuffer(buffer);

				oldCtx = MemoryContextSwitchTo(state->tmpCtx);
				// COW
				for (i = FirstOffsetNumber; i <= max; i++) {
					entry = start;
					// TOOD HnswMemTuple
					storageType* q = _getTupleArray(index, blkno, i);

					ItemPointerSet(&iptr, blkno, i);
					binaryheap* candidates = _search_level(&state->blstate, 
						state->blstate.ef_construction, l, q, &entry);
					// set links
					bidirection_connect2(&state->blstate, candidates, iptr, l, true);
				
					binaryheap_free(candidates);

					pfree(q);
				}
				
				MemoryContextSwitchTo(oldCtx);
				MemoryContextReset(state->tmpCtx);
				blkno = nextblk;	
				
			}
		}
		
		
	}

	if (index_parallel == 0)
		return; 

	int nworkers = index_parallel;
	// parallel handle level 0
	/* prohibit unsafe state changes */
	EnterParallelMode();		
	
	ParallelContext *pcxt = CreateParallelContextForExternalFunction("quantum", "_build_L0index", nworkers);
	
    /* Estimate space for fixed-stimator, 1); */
	shm_toc_estimate_chunk(&pcxt->estimator,
				sizeof(HnswShared));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

    /* create DSM and copy state to it */
	InitializeParallelDSM(pcxt);	

	/* Store the data for which we reserved space. */
	HnswShared* pshared = shm_toc_allocate(pcxt->toc, sizeof(HnswShared));

	pshared->indexrelid = RelationGetRelid(index);
	//pshared->blstate = state->blstate;
	pshared->L1start = L1entry;
	pshared->L1ntuples = L1ntuples;
	pshared->L0pages = L0pages;

	SpinLockInit(&pshared->mutex);
	pshared->nworker = 0;
	pshared->blkno = blkno;
	pshared->done_pages = 0;
	shm_toc_insert(pcxt->toc, 1, pshared);

	LaunchParallelWorkers(pcxt);
	pshared->nprocess = pcxt->nworkers_launched;
	/* do parallel stuff */

	WaitForParallelWorkersToFinish(pcxt);

	/* read any final results from dynamic shared memory */

	DestroyParallelContext(pcxt);
	ExitParallelMode();

	//MemoryContextSwitchTo(oldCtx);
	//MemoryContextReset(state->tmpCtx);

}



IndexBuildResult *
blbuild_parallel(Relation heap, Relation index, IndexInfo *indexInfo)
{
	//elog(INFO, "blbuild entry");
	IndexBuildResult *result;
	double		reltuples;
	HnswBuildState buildstate;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			 RelationGetRelationName(index));

	/* Initialize the meta page */
	//START_CRIT_SECTION();
	HnswInitMetapage(index, true);
	//END_CRIT_SECTION();
	/* Initialize the bloom build state */
	//memset(&buildstate, 0, sizeof(buildstate));
	initHnswState(&buildstate.blstate, index);
	buildstate.blstate.isBuild = true;
	buildstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											  "Hnsw build temporary context",
											  ALLOCSET_LARGE_SIZES);
	initCachedPage(&buildstate);

	/* Do the heap scan */
	reltuples = IndexBuildScan(heap, index, indexInfo, true,
									   hnswBuildCallback2, (void *) &buildstate);

	/* Flush last page if needed (it will be, unless heap was empty) */
	if (buildstate.flush)
		flushCachedPage(index, &buildstate);

	

	MemoryContextDelete(buildstate.tmpCtx);

	buildstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											  "Hnsw build graph context",
											  ALLOCSET_LARGE_SIZES);

	build_graph(&buildstate, index);


	MemoryContextDelete(buildstate.tmpCtx);
	/*
	 * Write index to xlog
	 */
	for (int blkno = 0; blkno < RelationGetNumberOfBlocks(index); blkno++)
	{
		Buffer		buffer;
		GenericXLogState *state;

		CHECK_FOR_INTERRUPTS();

		buffer = _hnsw_getbuf(index, blkno, BUFFER_LOCK_EXCLUSIVE);
		

		state = GenericXLogStart(index);
		GenericXLogRegisterBuffer(state, buffer, GENERIC_XLOG_FULL_IMAGE);
		GenericXLogFinish(state);

		// UnlockReleaseBuffer(buffer);
		_hnsw_relbuf(index, buffer);
	}

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = reltuples;
	result->index_tuples = buildstate.indtuples;

	return result;
}



/*
 * Build an empty bloom index in the initialization fork.
 */
void
blbuildempty(Relation index)
{
	elog(INFO, "blbuildempty entry");
	HnswInitMetapage(index, false);
}

/*
 * Insert new tuple to the bloom index.
 */
bool
blinsert(Relation index, Datum *values, bool *isnull,
		 ItemPointer ht_ctid, Relation heapRel,
		 IndexUniqueCheck checkUnique)
{
	HnswBuildState buildstate;
	/* Initialize the hnsw build state */
	initHnswState(&buildstate.blstate, index);
	
	buildstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											  "Hnsw insert temporary context",
											  ALLOCSET_LARGE_SIZES);	
	initCachedPage(&buildstate);

	_hnsw_insert(index, values, isnull, ht_ctid, (void *) &buildstate, true);

	if (buildstate.flush)
		flushCachedPage(index, &buildstate);

	MemoryContextDelete(buildstate.tmpCtx);
	return false;
}


IndexBuildResult *
blbuild(Relation heap, Relation index, IndexInfo *indexInfo) {
	if (index_parallel == 0) {
		return blbuild_seq(heap, index, indexInfo);
	} else {
		return blbuild_parallel(heap, index, indexInfo);
	}
}
