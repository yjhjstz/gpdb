/*
 * spgistfuncs.c
 *		Functions to investigate the content of SP-GiST indexes
 *
 * Source code is based on Gevel module available at
 * (http://www.sai.msu.su/~megera/oddmuse/index.cgi/Gevel)
 * Originally developed by:
 *  - Oleg Bartunov <oleg@sai.msu.su>, Moscow, Moscow University, Russia
 *  - Teodor Sigaev <teodor@sigaev.ru>, Moscow, Moscow University, Russia
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/pageinspect/spgistfuncs.c
 */

#include "postgres.h"

#include <assert.h>
#include <math.h>

#if PG_VERSION_NUM < 120000
#include "nodes/relation.h"
#include "utils/builtins.h"
#include "utils/array.h"
#else
#include "access/relation.h"
#include "utils/varlena.h"
#include <utils/float.h>
#include "common/shortest_dec.h"

#endif


#include "access/relscan.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/datum.h"

#include "utils/rel.h"
#include "storage/bufmgr.h"

#include "hnsw.h"


#define IsIndex(r) ((r)->rd_rel->relkind == RELKIND_INDEX)


PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(hnsw_stats);
PG_FUNCTION_INFO_V1(array_distance);
PG_FUNCTION_INFO_V1(array_inner_product);
PG_FUNCTION_INFO_V1(linear);

/*
 * hnsw_stats
 *		Show statistics about hnsw index
 *
 * Usage:
 *		select hnsw_stats('tt_idx', 0);
 */
Datum
hnsw_stats(PG_FUNCTION_ARGS)
{
	text	   *name = PG_GETARG_TEXT_P(0);
  int      id = PG_GETARG_INT32(1);
	RangeVar   *relvar;
	Relation	rel;
  HnswMetaPage metap;
	BlockNumber blkno;
	BlockNumber totalPages = 0,
				innerPages = 0,
				leafPages = 0,
				deletedPages = 0;
	int 		count = 0;
	char		res[1024];
  int    max_level;

	relvar = makeRangeVarFromNameList(textToQualifiedNameList(name));
	rel = relation_openrv(relvar, AccessShareLock);

	if (!IsIndex(rel))
		elog(ERROR, "relation \"%s\" is not an SPGiST index",
			 RelationGetRelationName(rel));

	totalPages = RelationGetNumberOfBlocks(rel);
  Buffer  metabuf = InvalidBuffer;
  metap = _getcachedmetap(rel, &metabuf, true);
  elog(INFO, "level %d, algorithm %d.", metap->max_level_, metap->nproc);
  max_level = metap->max_level_;
  if (BufferIsValid(metabuf))
    _hnsw_dropbuf(rel, metabuf);

	for (blkno = HNSW_METAPAGE_BLKNO; blkno < totalPages; blkno++)
	{
		Buffer		buffer;
		Page		page;

		buffer = ReadBuffer(rel, blkno);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buffer);

		if (PageIsNew(page) || HnswPageIsDeleted(page))
		{
			deletedPages++;
			UnlockReleaseBuffer(buffer);
			continue;
		}

		if (HnswPageIsMeta(page))
		{
			innerPages++;
		}
		else if (HnswPageIsBucket(page))
		{
			innerPages++;

      HnswBucketData *it = HnswPageGetBucket(page);
      if (it->level > max_level) {
        UnlockReleaseBuffer(buffer);
        continue;
      }
      elog(INFO, "level=%d, ntuples=%d, pages=%d, first_free=%d, entry(%d,%d)",
          it->level, it->ntuples,
          it->pages,
          it->first_free,
          ItemPointerGetBlockNumber(&it->entry),
          ItemPointerGetOffsetNumber(&it->entry));
      
		}
		else if (HnswPageIsOverflow(page))
		{
			int			i,j,
						max;

			leafPages++;
			max = PageGetMaxOffsetNumber(page);
			
			for (i = FirstOffsetNumber; i <= max; i++)
			{
        bool print_next;
				HnswTuple* it;

				it = (HnswTuple*) PageGetItem(page,
											PageGetItemId(page, i));
        print_next = (it->level == 0);

      #if 1
        if ((id != 0 && it->id == id) || (id == 0)) {
          elog(INFO, "id=%d(%c), level=%d, in=%d, out=%d, #(%d, %d), next-level[%d, %d]", 
            it->id, it->deleted?'x':'*', it->level, it->in_degree, it->out_degree,
            ItemPointerGetBlockNumber(&it->iptr),
            ItemPointerGetOffsetNumber(&it->iptr),
            print_next ? 0: ItemPointerGetBlockNumber(&it->next),
            print_next ? 0: ItemPointerGetOffsetNumber(&it->next));

          HnswNode* nodes = HnswGetTupleNodes(it);
          for ( j = 0; j < it->out_degree; j++) {
            elog(INFO, "d[%f]->(%d, %d)", nodes[j].distance,
              ItemPointerGetBlockNumber(&nodes[j].pointer),
              ItemPointerGetOffsetNumber(&nodes[j].pointer));
          } 
        }
				
       
      #else
        HnswNode* nodes = (HnswNode*) ((char*)it+it->offset_out_links);

        if (it->level == 2) {
          printf("[%d.%d,%d.%d,%d],\n", 
            ItemPointerGetBlockNumber(&it->iptr),
            ItemPointerGetOffsetNumber(&it->iptr),
            ItemPointerGetBlockNumber(&it->next),
            ItemPointerGetOffsetNumber(&it->next), 0);
        }
        

        HnswNode* nodes = (HnswNode*) ((char*)it+it->offset_out_links);
        for ( j = 0; j < it->out_degree; j++) {
          printf("[%d.%d,%d.%d,%f],\n", ItemPointerGetBlockNumber(&it->iptr),
            ItemPointerGetOffsetNumber(&it->iptr),
            ItemPointerGetBlockNumber(&nodes[j].pointer),
            ItemPointerGetOffsetNumber(&nodes[j].pointer),
            nodes[j].distance);
        }
        fflush(stdout);
      #endif
				
        count++;
			}
		} else {
      elog(INFO, "unknown pages.");
    }


		UnlockReleaseBuffer(buffer);
	}

	index_close(rel, AccessShareLock);

	snprintf(res, sizeof(res),
			 "totalPages:        %u\n"
			 "deletedPages:      %u\n"
			 "innerPages:        %u\n"
			 "leafPages:         %u\n"
			 "count:			 %u\n",
			 totalPages, deletedPages, innerPages, leafPages, count);

	PG_RETURN_TEXT_P(CStringGetTextDatum(res));
}



#if 0
float4
float4in_internal(char *num, char **endptr_p,
          const char *type_name, const char *orig_string) {

  char     *orig_num;
  float   val;
  char     *endptr;

  /*
   * endptr points to the first character _after_ the sequence we recognized
   * as a valid floating point number. orig_num points to the original input
   * string.
   */
  orig_num = num;

  /* skip leading whitespace */
  while (*num != '\0' && isspace((unsigned char) *num))
    num++;

  /*
   * Check for an empty-string input to begin with, to avoid the vagaries of
   * strtod() on different platforms.
   */
  if (*num == '\0')
    ereport(ERROR,
        (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
         errmsg("invalid input syntax for type %s: \"%s\"",
            "real", orig_num)));

  errno = 0;
  val = strtof(num, &endptr);

  /* did we not see anything that looks like a double? */
  if (endptr == num || errno != 0)
  {
    int     save_errno = errno;

    /*
     * C99 requires that strtof() accept NaN, [+-]Infinity, and [+-]Inf,
     * but not all platforms support all of these (and some accept them
     * but set ERANGE anyway...)  Therefore, we check for these inputs
     * ourselves if strtof() fails.
     *
     * Note: C99 also requires hexadecimal input as well as some extended
     * forms of NaN, but we consider these forms unportable and don't try
     * to support them.  You can use 'em if your strtof() takes 'em.
     */
    if (pg_strncasecmp(num, "NaN", 3) == 0)
    {
      val = get_float4_nan();
      endptr = num + 3;
    }
    else if (pg_strncasecmp(num, "Infinity", 8) == 0)
    {
      val = get_float4_infinity();
      endptr = num + 8;
    }
    else if (pg_strncasecmp(num, "+Infinity", 9) == 0)
    {
      val = get_float4_infinity();
      endptr = num + 9;
    }
    else if (pg_strncasecmp(num, "-Infinity", 9) == 0)
    {
      val = -get_float4_infinity();
      endptr = num + 9;
    }
    else if (pg_strncasecmp(num, "inf", 3) == 0)
    {
      val = get_float4_infinity();
      endptr = num + 3;
    }
    else if (pg_strncasecmp(num, "+inf", 4) == 0)
    {
      val = get_float4_infinity();
      endptr = num + 4;
    }
    else if (pg_strncasecmp(num, "-inf", 4) == 0)
    {
      val = -get_float4_infinity();
      endptr = num + 4;
    }
    else if (save_errno == ERANGE)
    {
      /*
       * Some platforms return ERANGE for denormalized numbers (those
       * that are not zero, but are too close to zero to have full
       * precision).  We'd prefer not to throw error for that, so try to
       * detect whether it's a "real" out-of-range condition by checking
       * to see if the result is zero or huge.
       *
       * Use isinf() rather than HUGE_VALF on VS2013 because it
       * generates a spurious overflow warning for -HUGE_VALF.  Also use
       * isinf() if HUGE_VALF is missing.
       */
      if (val == 0.0 ||
#if !defined(HUGE_VALF) || (defined(_MSC_VER) && (_MSC_VER < 1900))
        isinf(val)
#else
        (val >= HUGE_VALF || val <= -HUGE_VALF)
#endif
        )
        ereport(ERROR,
            (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
             errmsg("\"%s\" is out of range for type real",
                orig_num)));
    }
    else
      ereport(ERROR,
          (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
           errmsg("invalid input syntax for type %s: \"%s\"",
              "real", orig_num)));
  }
#ifdef HAVE_BUGGY_SOLARIS_STRTOD
  else
  {
    /*
     * Many versions of Solaris have a bug wherein strtod sets endptr to
     * point one byte beyond the end of the string when given "inf" or
     * "infinity".
     */
    if (endptr != num && endptr[-1] == '\0')
      endptr--;
  }
#endif              /* HAVE_BUGGY_SOLARIS_STRTOD */

  /* skip trailing whitespace */
  while (*endptr != '\0' && isspace((unsigned char) *endptr))
    endptr++;

  /* if there is any junk left at the end of the string, bail out */
  if (endptr_p)
    *endptr_p = endptr;
  else if (*endptr != '\0')
    ereport(ERROR,
        (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
         errmsg("invalid input syntax for type %s: \"%s\"",
            "real", orig_num)));


  /*
   * if we get here, we have a legal double, still need to check to see if
   * it's a legal float4
   */
  

  return ((float4) val);
}




char *
float4out_internal(float4 num)
{
	char	   *ascii = (char *) palloc(32);
	int			ndig = FLT_DIG + extra_float_digits;

	if (extra_float_digits > 0)
	{
		float_to_shortest_decimal_buf(num, ascii);
		return (ascii);
	}

	(void) pg_strfromd(ascii, 32, ndig, num);
  	return ascii;

}
#endif




Datum
array_distance(PG_FUNCTION_ARGS)
{
  ArrayType      *a = PG_GETARG_ARRAYTYPE_P(0);
  ArrayType      *b = PG_GETARG_ARRAYTYPE_P(1);
  int  dim;
  double     distance;
  dim = ARRNELEMS(a);
  
  Assert(ARRNELEMS(b) == dim);

  distance = fvec_L2sqr(ARRPTR(a), ARRPTR(b), dim);
  
  PG_FREE_IF_COPY(a, 0);
  PG_FREE_IF_COPY(b, 1);

  PG_RETURN_FLOAT8(sqrt(distance));
}



Datum
array_inner_product(PG_FUNCTION_ARGS)
{
  ArrayType      *a = PG_GETARG_ARRAYTYPE_P(0);
  ArrayType      *b = PG_GETARG_ARRAYTYPE_P(1);
  int  dim;
  double     distance;
  dim = ARRNELEMS(a);

  Assert(ARRNELEMS(b) == dim);

  distance = fvec_inner_product(ARRPTR(a), ARRPTR(b), dim);
  
  PG_FREE_IF_COPY(a, 0);
  PG_FREE_IF_COPY(b, 1);

  PG_RETURN_FLOAT8(distance);
}


Datum
linear(PG_FUNCTION_ARGS)
{
  float  distance = PG_GETARG_FLOAT4(0);
  float  bias = PG_GETARG_FLOAT4(1);
  double res = 1.0 * distance + 0 * bias;

  PG_RETURN_FLOAT8(res);
}
