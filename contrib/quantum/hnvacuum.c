/*-------------------------------------------------------------------------
 *
 * blvacuum.c
 *		hnsw VACUUM functions.
 *
 * Copyright (c) 2016-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/quantum/hnvacuum.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "catalog/storage.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"

#include "hnsw.h"
/*
 * Bulk deletion of all index entries pointing to a set of heap tuples.
 * The set of target tuples is specified via a callback routine that tells
 * whether any given heap tuple (identified by ItemPointer) is being deleted.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
IndexBulkDeleteResult *
blbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			 IndexBulkDeleteCallback callback, void *callback_state)
{
	elog(INFO, "blbulkdelete");
	Relation	index = info->index;
	BlockNumber blkno,
				npages;
	int			i, max = 0;
	HnswState	state;
	Buffer		buffer;
	Page		page;
	//HnswMetaPageData *metaData;
	GenericXLogState *gxlogState;

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	initHnswState(&state, index);

	/*
	 * Iterate over the pages. We don't care about concurrently added pages,
	 * they can't contain tuples to delete.
	 */
	npages = RelationGetNumberOfBlocks(index);
	for (blkno = HNSW_METAPAGE_BLKNO; blkno < npages; blkno++)
	{
		bool change = false;
		HnswTuple *itup;

		vacuum_delay_point();

		buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, info->strategy);

		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		gxlogState = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(gxlogState, buffer, 0);

		/* Ignore empty/deleted pages until blvacuumcleanup() */
		if (PageIsNew(page) || HnswPageIsDeleted(page) || HnswPageIsMeta(page) || HnswPageIsBucket(page))
		{
			UnlockReleaseBuffer(buffer);
			GenericXLogAbort(gxlogState);
			continue;
		}
		
		/*
		 * Iterate over the tuples.  itup points to current tuple being
		 * scanned, itupPtr points to where to save next non-deleted tuple.
		 */		
		max = PageGetMaxOffsetNumber(page);
		for (i = FirstOffsetNumber; i <= max; i++) {
			itup = (HnswTuple*) PageGetItem(page, PageGetItemId(page, i));
			if (callback(&itup->heapPtr, callback_state)) {
				//TODO, just marked
				itup->deleted = true;
				stats->tuples_removed += 1;
				change = true;
			}
    	}


		/* Did we delete something? */
		if (change)
		{
			/* Is it empty page now? */
			if (PageGetMaxOffsetNumber(page) == 0)
				HnswPageSetDeleted(page);
			/* Finish WAL-logging */
			GenericXLogFinish(gxlogState);
		}
		else
		{
			/* Didn't change anything: abort WAL-logging */
			GenericXLogAbort(gxlogState);
		}
		UnlockReleaseBuffer(buffer);
	}
#if 0
	/*
	 * Update the metapage's notFullPage list with whatever we found.  Our
	 * info could already be out of date at this point, but blinsert() will
	 * cope if so.
	 */
	buffer = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

	gxlogState = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(gxlogState, buffer, 0);

	metaData = HnswPageGetMeta(page);
	// TODO 

	GenericXLogFinish(gxlogState);
	UnlockReleaseBuffer(buffer);
#endif
	return stats;
}

/*
 * Post-VACUUM cleanup.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
IndexBulkDeleteResult *
blvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	elog(INFO, "blvacuumcleanup");
	Relation	index = info->index;
	BlockNumber npages,
				blkno;

	if (info->analyze_only)
		return stats;

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	/*
	 * Iterate over the pages: insert deleted pages into FSM and collect
	 * statistics.
	 */
	npages = RelationGetNumberOfBlocks(index);
	stats->num_pages = npages;
	stats->pages_free = 0;
	stats->num_index_tuples = 0;
	for (blkno = HNSW_METAPAGE_BLKNO; blkno < npages; blkno++)
	{
		Buffer		buffer;
		Page		page;

		vacuum_delay_point();

		buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, info->strategy);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = (Page) BufferGetPage(buffer);

		if (PageIsNew(page) || HnswPageIsDeleted(page))
		{
			RecordFreeIndexPage(index, blkno);
			stats->pages_free++;
		}
		else if (HnswPageIsMeta(page) || HnswPageIsBucket(page))
		{
			// do nothing
		} else {
			stats->num_index_tuples += PageGetMaxOffsetNumber(page);
		}

		UnlockReleaseBuffer(buffer);
	}

	IndexFreeSpaceMapVacuum(info->index);

	return stats;
}

bool
blvalidate(Oid opclassoid) {
	return true;
}
