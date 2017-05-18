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

/* History record text constants */
#define HIST_MSG_MAXSZ 256
#define HIST_MSG_CRON_SCHEDULED "{\"event\": \"scheduled\", \"schedule\": \"%s\", \"command\": \"%s\"}"
#define HIST_MSG_CRON_UNSCHEDULED "{\"event\": \"unscheduled\", \"command\": \"%s\"}"
#define HIST_MSG_CRON_STARTED "{\"event\": \"started\", \"command\": \"%s\"}"
#define HIST_MSG_CRON_COMPLETED "{\"event\": \"completed\", \"command\": \"%s\", \"command_status\": \"%s\", \"tuples\": \"%s\"}"
#define HIST_MSG_CRON_FAILED_WITH_MESSAGE "{\"event\": \"failed\", \"command\": \"%s\", \"message\": \"%s\"}"

/* utility functions */
extern void AddJobHistory(int64 jobId, const char *message, bool newTransaction);

#endif
