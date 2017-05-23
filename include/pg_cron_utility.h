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

/* History record json examples */
#define HIST_MSG_CRON_SCHEDULED "{\"event\": \"scheduled\", \"schedule\": \"%s\", \"command\": \"%s\"}"
#define HIST_MSG_CRON_UNSCHEDULED "{\"event\": \"unscheduled\", \"command\": \"%s\"}"
#define HIST_MSG_CRON_STARTED "{\"event\": \"started\", \"command\": \"%s\"}"
#define HIST_MSG_CRON_COMPLETED "{\"event\": \"completed\", \"command\": \"%s\", \"command_status\": \"%s\", \"tuple_count\": \"%s\"}"
#define HIST_MSG_CRON_COMPLETED_TUPLES "{\"event\": \"completed\", \"command\": \"%s\", \"message\": \"%d %s\"}"
#define HIST_MSG_CRON_FAILED "{\"event\": \"failed\", \"command\": \"%s\"}"
#define HIST_MSG_CRON_FAILED_WITH_MESSAGE "{\"event\": \"failed\", \"command\": \"%s\", \"message\": \"%s\"}"

/* utility functions */
extern void RecordJobScheduled(int64 jobId, const char *command, const char *schedule);
extern void RecordJobUnscheduled(int64 jobId, const char *command);
extern void RecordJobStarted(int64 jobId, const char *command);
extern void RecordJobCompleted(int64 jobId, const char *command, int tupleCount);
extern void RecordJobCompletedStatus(int64 jobId, const char *command, const char *commandStatus, const char *tupleCount);
extern void RecordJobFailed(int64 jobId, const char *command);
extern void RecordJobFailedMessage(int64 jobId, const char *command, const char *message);

#endif
