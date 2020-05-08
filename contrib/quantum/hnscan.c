/*-------------------------------------------------------------------------
 *
 * hnscan.c
 *		Hnsw index scan functions.
 *
 * Copyright (c) 2016-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/quantum/hnscan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"


#include "access/relscan.h"
#include "pgstat.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/array.h"
#include "executor/executor.h"

#include "hnsw.h"

#define int4_max(a, b) (((a) > (b)) ? (a) : (b))

extern int
pairingheap_HnswSearchItem_cmp(const pairingheap_node *a,
							   const pairingheap_node *b, void *arg);
extern void
hnswFreeSearchItem(HnswSearchItem * item);
extern HnswSearchItem *
hnswAllocSearchItem(ItemPointer pt, float dist);

/*
 * Begin scan of hnsw index.
 */
IndexScanDesc
blbeginscan(Relation r, int nkeys, int norderbys)
{
	//elog(INFO, "blbeginscan %d", norderbys);
	IndexScanDesc scan;
	HnswScanOpaque so;

	scan = RelationGetIndexScan(r, nkeys, norderbys);

	so = (HnswScanOpaque) palloc(sizeof(HnswScanOpaqueData));
	initHnswState(&so->state, scan->indexRelation);

	so->tempCxt = AllocSetContextCreate(CurrentMemoryContext,
										"hnsw search temporary context",
										ALLOCSET_LARGE_SIZES);
	so->queueCxt = AllocSetContextCreate(CurrentMemoryContext,
										"hnsw scan temporary context",
										ALLOCSET_LARGE_SIZES);
	scan->opaque = so;
	so->first_call = true;
	return scan;
}

/*
 * Rescan a hnsw index.
 */
void
blrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		 ScanKey orderbys, int norderbys)
{
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;
	MemoryContext oldCxt;
	int i = 0;
	float* dur;
	ScanKey skey = NULL;
	HeapTupleHeader query;
	bool isNull;

	oldCxt = MemoryContextSwitchTo(so->tempCxt);

	// TODO fix scan key
	if (scankey && scan->numberOfKeys > 0)
	{
		memmove(scan->keyData, scankey,
				scan->numberOfKeys * sizeof(ScanKeyData));
	}
	// TODO fix order key
	if (orderbys && scan->numberOfOrderBys > 0)
		memmove(scan->orderByData, orderbys,
				scan->numberOfOrderBys * sizeof(ScanKeyData));
	
	if (nscankeys > 0) {
		skey = &scan->keyData[0];
	}
	if (norderbys > 0) {
		skey = &scan->orderByData[0];
	}
	query = DatumGetHeapTupleHeader(skey->sk_argument);

	ArrayType* box = DatumGetArrayTypeP(GetAttributeByNum(query, 1, &isNull));
	so->threshold = DatumGetFloat4(GetAttributeByNum(query, 2, &isNull));
	so->topk = DatumGetInt32(GetAttributeByNum(query, 3, &isNull));
	
	dur = ARRPTR(box);
	so->dims = ARRNELEMS(box);
	so->q = (storageType*) palloc(sizeof(storageType) * so->dims);
	for (i = 0; i < so->dims; i++) {
		(so->q)[i] = dur[i];
	}

	MemoryContextSwitchTo(oldCxt);

	oldCxt = MemoryContextSwitchTo(so->queueCxt);
	so->queue = pairingheap_allocate(pairingheap_HnswSearchItem_cmp, scan);
	MemoryContextSwitchTo(oldCxt);
}

/*
 * End scan of hnsw index.
 */
void
blendscan(IndexScanDesc scan)
{
	//Relation	rel = scan->indexRelation;
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;
	//HnswMetaPage metap;
	if (so->q) {
		pfree(so->q);
	}
	if (so->queue) {
		pairingheap_free(so->queue);
	}

	MemoryContextReset(so->tempCxt);
	MemoryContextDelete(so->tempCxt);
	MemoryContextReset(so->queueCxt);
	MemoryContextDelete(so->queueCxt);

	pfree(so);
}



bool
_hnsw_next(IndexScanDesc scan, ScanDirection dir)
{
	//elog(INFO, "_hnsw_next ss");
	Page page;
	Relation	rel = scan->indexRelation;
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;
	HnswSearchItem* pitem;
	HnswNode* node;
	HnswTuple* cur;

	if (pairingheap_is_empty(so->queue)) {
		return false;
	} 

	pitem = (HnswSearchItem*) pairingheap_first(so->queue);
	node = pitem->value;
	cur = _getHnswTuple(&so->state, &node->pointer);
	page = BufferGetPage(cur->buf);
	TestForOldSnapshot(scan->xs_snapshot, rel, page);
	SET_SCAN_TID(scan, cur->heapPtr);
	_hnsw_dropbuf(rel, cur->buf);
	pairingheap_remove_first(so->queue);
	hnswFreeSearchItem(pitem);
	//elog(INFO, "_hnsw_next end");
	return true;
}



bool
_hnsw_first(IndexScanDesc scan, ScanDirection dir)
{
	//elog(INFO, "_hnsw_first start");
	HnswMetaPage metap;
	MemoryContext oldCtx;
	HnswBucketData* bucketp;
	int l, lc;
	Buffer bucketbuf;
	Buffer	metabuf = InvalidBuffer;
	Page page;
	ItemPointerData start;
	HnswSearchItem* pitem;
	HnswNode* node;
	HnswTuple* cur;
	Relation	index = scan->indexRelation;
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;
	
	oldCtx = MemoryContextSwitchTo(so->queueCxt);

	metap = _getcachedmetap(index, &metabuf, true);
	Assert(metap);
	metap->search_count = 0;

	if (metap->dims != so->dims) {
		elog(ERROR , "dims not match.");
		return false;
	}

	lc = metap->max_level_;
	ItemPointerSetInvalid(&start);
	if (lc != INVALID_LEVEL) {
		// search layers
		for (l = lc; l >= 1; l--) {
			bucketbuf = _hnsw_getbuf(index, metap->level_blk[l], BUFFER_LOCK_SHARE);
			bucketp = HnswPageGetBucket(BufferGetPage(bucketbuf));
			if (!ItemPointerIsValid(&start)) {
				start = bucketp->entry;
			}
			Assert(ItemPointerIsValid(&start));
			start = _greedy_search(&so->state, bucketp->ntuples, so->q, &start);
			//print("search", l, &start);
			_hnsw_relbuf(index, bucketbuf);
		}

	}
	
	if (ItemPointerIsValid(&start)) {
	
		binaryheap* candidates = _search_level(&so->state, 
			int4_max(so->state.efsearch, so->topk), 0, so->q, &start);
		while (!binaryheap_empty(candidates)) {
			node = (HnswNode *) DatumGetPointer(binaryheap_first(candidates));
			pitem = hnswAllocSearchItem(&node->pointer, node->distance);
			binaryheap_remove_first(candidates);
			// pop max distance element
			// if (candidates->bh_size >= so->topk) {
			// 	continue;
			// }
			
			pairingheap_add(so->queue, &pitem->phNode);
		}

		binaryheap_free(candidates);
	}

	// release meta pin
	if (BufferIsValid(metabuf))
		_hnsw_dropbuf(index, metabuf);

	MemoryContextSwitchTo(oldCtx);

	if (pairingheap_is_empty(so->queue)) {
		return false;
	} 

	pitem = (HnswSearchItem*) pairingheap_first(so->queue);
	node = pitem->value;
	cur = _getHnswTuple(&so->state, &node->pointer);
	page = BufferGetPage(cur->buf);
	TestForOldSnapshot(scan->xs_snapshot, index, page);
	SET_SCAN_TID(scan, cur->heapPtr);
	_hnsw_dropbuf(index, cur->buf);
	pairingheap_remove_first(so->queue);
	hnswFreeSearchItem(pitem);

	//elog(INFO, "_hnsw_first end");
	return true;
}

#if 0
/*
 * Insert all matching tuples into a bitmap.
 */
int64
blgetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	int64		ntids = 0;
	bool res;
	//HnswScanOpaque so = (HnswScanOpaque) scan->opaque;

	res = _hnsw_first(scan, ForwardScanDirection);
	if (res) {
		tbm_add_tuples(tbm, &scan->xs_heaptid, 1, true);
		ntids++;
	}
	do {
		res = _hnsw_next(scan, ForwardScanDirection);
		if (res) {
			tbm_add_tuples(tbm, &scan->xs_heaptid, 1, true);
			ntids++;
		}
	} while(res);

	return ntids;
}
#endif

bool
blgettuple(IndexScanDesc scan, ScanDirection dir)
{
	
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;
	bool		res;
	//elog(INFO, "blgettuple %d", so->count++);
	scan->xs_recheck = false;

	/*
	 * If we've already initialized this scan, we can just advance it in the
	 * appropriate direction.  If we haven't done so yet, we call a routine to
	 * get the first item in the scan.
	 */
	if (so->first_call) {
		res = _hnsw_first(scan, dir);
		so->first_call = false;
	}
	else
	{
		/*
		 * Now continue the scan.
		 */
		res = _hnsw_next(scan, dir);
	}

	return res;
}

PG_FUNCTION_INFO_V1(array_ann);
Datum
array_ann(PG_FUNCTION_ARGS)
{
	const float EPSINON = 0.00001;
	Datum value = PG_GETARG_DATUM(0);
	HeapTupleHeader query = PG_GETARG_HEAPTUPLEHEADER(1);
	Datum queryDatum;
	float threshold;
	float distance;
	bool isNull;

	ArrayType* box = DatumGetArrayTypeP(value);
	queryDatum = GetAttributeByNum(query, 1, &isNull);
	ArrayType* q = DatumGetArrayTypeP(queryDatum);
	threshold = DatumGetFloat4(GetAttributeByNum(query, 2, &isNull));
	// L2 distance
	if ( (threshold >= - EPSINON) && (threshold <= EPSINON) ) {
		PG_RETURN_BOOL(true);
	} else {
		distance = fvec_inner_product(ARRPTR(q), ARRPTR(box), ARRNELEMS(box));
		if (distance >= threshold)
			PG_RETURN_BOOL(true);
	}
	PG_RETURN_BOOL(false);

}

