/*-------------------------------------------------------------------------
 *
 * hnpage.c
 *	  Hash table page management code for the Postgres hash access method
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  contrib/quantum/hnpage.c
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "storage/predicate.h"


#include "hnsw.h"

void HnswFillBucketPage(Relation index, Page page, int16 level, int dims);

void _hnsw_pageinit(Page page, Size size);

/*
 *	_hnsw_getbuf() -- Get a buffer by block number for read or write.
 *
 */
Buffer
_hnsw_getbuf(Relation rel, BlockNumber blkno, int access)
{
	Buffer		buf;

	if (blkno == P_NEW)
		elog(ERROR, "hnsw AM does not use P_NEW");

	buf = ReadBuffer(rel, blkno);
	//elog(INFO, "read buf# %d", blkno);

	if (access != HNSW_NOLOCK)
		LockBuffer(buf, access);

	/* ref count and lock type are correct */

	//_hnsw_checkpage(rel, buf, flags);

	return buf;
}


/*
 *	_hnsw_initbuf() -- Get and initialize a buffer by bucket number.
 */
void
_hnsw_initbuf(Page page, uint32 flags, int16 level, bool initpage)
{
	HnswPageOpaque opaque;
	// Page		page;

	// page = BufferGetPage(buf);

	/* initialize the page */
	if (initpage)
		_hnsw_pageinit(page, BLCKSZ);

	opaque = HnswPageGetOpaque(page);
	memset(opaque, 0, sizeof(HnswPageOpaqueData));

	opaque->hnsw_prevblkno = InvalidBlockNumber;
	opaque->hnsw_nextblkno = InvalidBlockNumber;
	opaque->level = level;
	opaque->maxoff = 0;
	opaque->flags = flags;
	opaque->hnsw_page_id = HNSW_PAGE_ID;
}



/*
 *	_hnsw_getbuf_with_strategy() -- Get a buffer with nondefault strategy.
 *
 *		This is identical to _hash_getbuf() but also allows a buffer access
 *		strategy to be specified.  We use this for VACUUM operations.
 */
Buffer
_hnsw_getbuf_with_strategy(Relation rel, BlockNumber blkno,
						   int access, int flags,
						   BufferAccessStrategy bstrategy)
{
	Buffer		buf;

	if (blkno == P_NEW)
		elog(ERROR, "hnsw AM does not use P_NEW");

	buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, bstrategy);

	if (access != HNSW_NOLOCK)
		LockBuffer(buf, access);

	/* ref count and lock type are correct */

	return buf;
}

/*
 * Allocate a new page (either by recycling, or by extending the index file)
 * The returned buffer is already pinned and exclusive-locked
 * Caller is responsible for initializing the page by calling BloomInitBuffer
 */
Buffer
HnswNewBuffer(Relation index)
{
	Buffer		buffer;
	bool		needLock;

	/* First, try to get a page from FSM */
	for (;;)
	{
		BlockNumber blkno = GetFreeIndexPage(index);

		if (blkno == InvalidBlockNumber)
			break;

		buffer = ReadBuffer(index, blkno);

		/*
		 * We have to guard against the possibility that someone else already
		 * recycled this page; the buffer may be locked if so.
		 */
		if (ConditionalLockBuffer(buffer))
		{
			Page		page = BufferGetPage(buffer);

			if (PageIsNew(page))
				return buffer;	/* OK to use, if never initialized */

			if (HnswPageIsDeleted(page))
				return buffer;	/* OK to use */

			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		}

		/* Can't use it, so release buffer and try again */
		ReleaseBuffer(buffer);
	}

	/* Must extend the file */
	needLock = !RELATION_IS_LOCAL(index);
	if (needLock)
		LockRelationForExtension(index, ExclusiveLock);

	buffer = ReadBuffer(index, P_NEW);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

	if (needLock)
		UnlockRelationForExtension(index, ExclusiveLock);

	return buffer;
}


/*
 *	_hnsw_relbuf() -- release a locked buffer.
 *
 * Lock and pin (refcount) are both dropped.
 */
void
_hnsw_relbuf(Relation rel, Buffer buf)
{
	UnlockReleaseBuffer(buf);
}

/*
 *	_hnsw_dropbuf() -- release an unlocked buffer.
 *
 * This is used to unpin a buffer on which we hold no lock.
 */
void
_hnsw_dropbuf(Relation rel, Buffer buf)
{
	ReleaseBuffer(buf);
}


/*
 *	_hnsw_pageinit() -- Initialize a new hnsw index page.
 */
void
_hnsw_pageinit(Page page, Size size)
{
	PageInit(page, size, sizeof(HnswPageOpaqueData));
}


/*
 * Fill in metapage for Hnsw index.
 */
void
HnswFillBucketPage(Relation index, Page page, int16 level, int dims)
{
	HnswBucketData *bkdata;

	/*
	 * Initialize contents of meta page, including a copy of the options,
	 * which are now frozen for the life of the index.
	 */
	_hnsw_initbuf(page, HNSW_BUCKET, level, true);
	bkdata = HnswPageGetBucket(page);
	memset(bkdata, 0, sizeof(HnswBucketData));
	
	bkdata->level = level;
	bkdata->dims = dims;
	bkdata->ntuples = 0;
	bkdata->pages = 0;
	bkdata->first_free = InvalidBlockNumber;
	bkdata->first_full = InvalidBlockNumber;
	ItemPointerSetInvalid(&bkdata->entry);
	((PageHeader) page)->pd_lower += sizeof(HnswBucketData);

	/* If this fails, probably FreeBlockNumberArray size calc is wrong: */
	Assert(((PageHeader) page)->pd_lower <= ((PageHeader) page)->pd_upper);
}

/*
 * Construct a default set of Hnsw options.
 */
static HnswOptions *
makeDefaultHnswOptions(void)
{
#define ALGO_LEN 8
	HnswOptions *opts;
	opts = (HnswOptions *) palloc0(sizeof(HnswOptions) + ALGO_LEN);
	opts->max_links = 16;
	opts->ef_construction = 100;
	opts->efsearch = 64;
	opts->dims = 128;
	strncpy(opts->algo, "l2\0", 3);
	SET_VARSIZE(opts, sizeof(HnswOptions) + ALGO_LEN);
	return opts;
#undef ALGO_LEN
}





void
HnswUpdateMetapage(Relation index, int maxlevel, bool isBuild)
{
	Buffer		metaBuffer;
	Page		metaPage;
	HnswMetaPageData *meta;
	GenericXLogState *state;
	/*
	 * Make a new page; since it is first page it should be associated with
	 * block number 0 (HNSW_METAPAGE_BLKNO).
	 */
	metaBuffer = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
	LockBuffer(metaBuffer, BUFFER_LOCK_EXCLUSIVE);
	Assert(BufferGetBlockNumber(metaBuffer) == HNSW_METAPAGE_BLKNO);
	if (isBuild) {
		metaPage = BufferGetPage(metaBuffer);
	} else {
		state = GenericXLogStart(index);
		metaPage = GenericXLogRegisterBuffer(state, metaBuffer, 0);
	}
	
	meta = HnswPageGetMeta(metaPage);
	//meta = HnswPageGetMeta(metaPage);
	meta->max_level_ = maxlevel;
	elog(INFO, "flush max level %d.", maxlevel);

	if (isBuild)
		MarkBufferDirty(metaBuffer);
	else
		GenericXLogFinish(state);

	UnlockReleaseBuffer(metaBuffer);
}

/*
 * Initialize metapage for hnsw index, call only once.
 */
void
HnswInitMetapage(Relation index, bool isBuild)
{
	Buffer		metaBuffer;
	Page		metaPage, bucketPage;
	GenericXLogState *state, *state2;
	HnswOptions *opts;
	HnswMetaPageData *metadata;
	int i;
	Buffer bucketbuf;
	/*
	 * Make a new page; since it is first page it should be associated with
	 * block number 0 (HNSW_METAPAGE_BLKNO).
	 */
	metaBuffer = HnswNewBuffer(index);
	Assert(BufferGetBlockNumber(metaBuffer) == HNSW_METAPAGE_BLKNO);

	/* Initialize contents of meta page */
	state = GenericXLogStart(index);
	/*
	 * Choose the index's options.  If reloptions have been assigned, use
	 * those, otherwise create default options.
	 */
	opts = (HnswOptions *) index->rd_options;
	if (!opts)
		opts = makeDefaultHnswOptions();
	if (isBuild)
		metaPage = BufferGetPage(metaBuffer);
	else
		metaPage = GenericXLogRegisterBuffer(state, metaBuffer,
											 GENERIC_XLOG_FULL_IMAGE);

	_hnsw_initbuf(metaPage, HNSW_META, INVALID_LEVEL, true);
	metadata = HnswPageGetMeta(metaPage);
	memset(metadata, 0, sizeof(HnswMetaPageData));
	metadata->magic = HNSW_MAGICK_NUMBER;

	metadata->maxM_ = opts->max_links;
	metadata->maxM0_ = 2 * opts->max_links;
	metadata->ef_construction_ = opts->ef_construction;
	metadata->efsearch_ = opts->efsearch;
	metadata->dims = opts->dims;
	metadata->max_level_ = INVALID_LEVEL;
	if (strncmp(opts->algo, "l2", 2) == 0) {
		metadata->nproc = 1;
	} else if (strncmp(opts->algo, "dot", 3) == 0) {
		metadata->nproc = 2;
	} else if (strncmp(opts->algo, "linear", 6) == 0) {
		metadata->nproc = 3;
		link_nearest = true;
	} else {
		elog(ERROR, "bad algorithm %s.", opts->algo);
	}
	((PageHeader) metaPage)->pd_lower += sizeof(HnswMetaPageData);
	

	for (i = 0; i < MAX_LEVEL; i++) {
		state2 = GenericXLogStart(index);
		bucketbuf = HnswNewBuffer(index);
		metadata->level_blk[i] = BufferGetBlockNumber(bucketbuf);
		if (isBuild)
			bucketPage = BufferGetPage(bucketbuf);
		else 
			bucketPage = GenericXLogRegisterBuffer(state2, bucketbuf,
										 GENERIC_XLOG_FULL_IMAGE);
		HnswFillBucketPage(index, bucketPage, i, opts->dims);
		if (isBuild)
			MarkBufferDirty(bucketbuf);
		else
			GenericXLogFinish(state2);
		UnlockReleaseBuffer(bucketbuf);

	}
	if (isBuild)
		MarkBufferDirty(metaBuffer);
	else
		GenericXLogFinish(state);
	
	UnlockReleaseBuffer(metaBuffer);
	
}


HnswMetaPage
_getcachedmetap(Relation rel, Buffer *metabuf, bool force_refresh)
{
	Page		page;

	Assert(metabuf);
	if (force_refresh || rel->rd_amcache == NULL)
	{
		char	   *cache = NULL;

		/*
		 * It's important that we don't set rd_amcache to an invalid value.
		 * Either MemoryContextAlloc or _hash_getbuf could fail, so don't
		 * install a pointer to the newly-allocated storage in the actual
		 * relcache entry until both have succeeeded.
		 */
		if (rel->rd_amcache == NULL)
			cache = MemoryContextAlloc(rel->rd_indexcxt,
									   sizeof(HnswMetaPageData));

		/* Read the metapage. */
		*metabuf = ReadBuffer(rel, HNSW_METAPAGE_BLKNO);
		LockBuffer(*metabuf, BUFFER_LOCK_SHARE);
		
		page = BufferGetPage(*metabuf);

		/* Populate the cache. */
		if (rel->rd_amcache == NULL)
			rel->rd_amcache = cache;
		memcpy(rel->rd_amcache, HnswPageGetMeta(page), sizeof(HnswMetaPageData));

		/* Release metapage lock, but keep the pin. */
		LockBuffer(*metabuf, BUFFER_LOCK_UNLOCK);
	}

	return (HnswMetaPage) rel->rd_amcache;
}
