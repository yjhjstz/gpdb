/*-------------------------------------------------------------------------
 *
 * progress.h
 *	  Constants used with the progress reporting facilities defined in
 *	  pgstat.h.  These are possibly interesting to extensions, so we
 *	  expose them via this header file.  Note that if you update these
 *	  constants, you probably also need to update the views based on them
 *	  in system_views.sql.
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/progress.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PROGRESS_H
#define PROGRESS_H

/* Progress parameters for (lazy) vacuum */
#define PROGRESS_VACUUM_PHASE					0
#define PROGRESS_VACUUM_TOTAL_HEAP_BLKS			1
#define PROGRESS_VACUUM_HEAP_BLKS_SCANNED		2
#define PROGRESS_VACUUM_HEAP_BLKS_VACUUMED		3
#define PROGRESS_VACUUM_NUM_INDEX_VACUUMS		4
#define PROGRESS_VACUUM_MAX_DEAD_TUPLES			5
#define PROGRESS_VACUUM_NUM_DEAD_TUPLES			6

/* Phases of vacuum (as advertised via PROGRESS_VACUUM_PHASE) */
#define PROGRESS_VACUUM_PHASE_SCAN_HEAP			1
#define PROGRESS_VACUUM_PHASE_VACUUM_INDEX		2
#define PROGRESS_VACUUM_PHASE_VACUUM_HEAP		3
#define PROGRESS_VACUUM_PHASE_INDEX_CLEANUP		4
#define PROGRESS_VACUUM_PHASE_TRUNCATE			5
#define PROGRESS_VACUUM_PHASE_FINAL_CLEANUP		6

/* Progress parameters for CREATE INDEX */
/* 3, 4 and 5 reserved for "waitfor" metrics */
#define PROGRESS_CREATEIDX_COMMAND              0
#define PROGRESS_CREATEIDX_INDEX_OID            6
#define PROGRESS_CREATEIDX_ACCESS_METHOD_OID    8
#define PROGRESS_CREATEIDX_PHASE                9   /* AM-agnostic phase # */
#define PROGRESS_CREATEIDX_SUBPHASE             10  /* phase # filled by AM */
#define PROGRESS_CREATEIDX_TUPLES_TOTAL         11
#define PROGRESS_CREATEIDX_TUPLES_DONE          12
#define PROGRESS_CREATEIDX_PARTITIONS_TOTAL     13
#define PROGRESS_CREATEIDX_PARTITIONS_DONE      14

#endif
