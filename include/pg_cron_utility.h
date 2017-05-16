/*-------------------------------------------------------------------------
 *
 * src/pg_cron_utility.h
 *
 * Utility functions.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_CRON_UTILITY_H
#define PG_CRON_UTILITY_H

/* global definitions and constants */
#define CRON_SCHEMA_NAME "cron"
#define EXTENSION_NAME "pg_cron"

#define Natts_cron_hist 4
#define Anum_cron_hist_histid 1
#define Anum_cron_hist_jobid 2
#define Anum_cron_hist_message 3
#define Anum_cron_hist_created_at 4

/* utility functions */
extern int64 NextSeqId(const char *sequence);
extern Oid CronExtensionOwner(void);
extern void AddValues(const char *rel, Datum *values, bool *isNulls);
extern void AddJobHistory(int64 jobId, char *message);

#endif
