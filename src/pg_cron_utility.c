/*-------------------------------------------------------------------------
 *
 * src/pg_cron.c
 *
 * Implementation of the pg_cron task scheduler.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "pg_cron_utility.h"
#include "pg_cron.h"

#include "access/xact.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_type.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "commands/sequence.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "executor/spi.h"
#include "utils/snapmgr.h"

#define HIST_TABLE_NAME "history"
#define HIST_ID_SEQUENCE_NAME "cron.histid_seq"

void RecordJobScheduled(int64 jobId, const char *command, const char *schedule)
{
  /* No new transaction, as we're already in one as part of cron.schedule() */
  const char *insert_template = "INSERT INTO %s.%s (jobid, message) VALUES ($1, jsonb_build_object('event', 'scheduled', 'command', $2, 'schedule', $3));";
  int nargs = 3;
  Oid argtypes[nargs];
  Datum values[nargs];
  int ret;
  int length = snprintf(NULL, 0, insert_template, CRON_SCHEMA_NAME, HIST_TABLE_NAME);
  char *insert = palloc((length + 1) * sizeof(char));
  snprintf(insert, length + 1, insert_template, CRON_SCHEMA_NAME, HIST_TABLE_NAME);

  memset(argtypes, 0, sizeof(argtypes));
  memset(values, 0, sizeof(values));

  argtypes[0] = INT8OID;
  argtypes[1] = TEXTOID;
  argtypes[2] = TEXTOID;

  values[0] = Int64GetDatum(jobId);
  values[1] = CStringGetTextDatum(command);
  values[2] = CStringGetTextDatum(schedule);

  SPI_connect();
  ret = SPI_execute_with_args(insert,
      nargs, argtypes, values,
      NULL, false, 0);
  if (ret != SPI_OK_INSERT)
  {
    ereport(ERROR, (errmsg("pg_cron history record entry failed, SPI error code %d", ret)));
  }
  SPI_finish();
}

void RecordJobScheduledAt(int64 jobId, const char *command, const char *at)
{
  /* No new transaction, as we're already in one as part of cron.schedule() */
  const char *insert_template = "INSERT INTO %s.%s (jobid, message) VALUES ($1, jsonb_build_object('event', 'scheduled-at', 'command', $2, 'at', $3));";
  int nargs = 3;
  Oid argtypes[nargs];
  Datum values[nargs];
  int ret;
  int length = snprintf(NULL, 0, insert_template, CRON_SCHEMA_NAME, HIST_TABLE_NAME);
  char *insert = palloc((length + 1) * sizeof(char));
  snprintf(insert, length + 1, insert_template, CRON_SCHEMA_NAME, HIST_TABLE_NAME);

  memset(argtypes, 0, sizeof(argtypes));
  memset(values, 0, sizeof(values));

  argtypes[0] = INT8OID;
  argtypes[1] = TEXTOID;
  argtypes[2] = TEXTOID;

  values[0] = Int64GetDatum(jobId);
  values[1] = CStringGetTextDatum(command);
  values[2] = CStringGetTextDatum(at);

  SPI_connect();
  ret = SPI_execute_with_args(insert,
      nargs, argtypes, values,
      NULL, false, 0);
  if (ret != SPI_OK_INSERT)
  {
    ereport(ERROR, (errmsg("pg_cron history record entry failed, SPI error code %d", ret)));
  }
  SPI_finish();
}

void RecordJobUnscheduled(int64 jobId, const char *command)
{
  /* No new transaction, as we're already in one as part of cron.schedule() */
  const char *insert_template = "INSERT INTO %s.%s (jobid, message) "
                                "VALUES ($1, jsonb_build_object('event', 'unscheduled', 'command', $2))";
  int nargs = 2;
  Oid argtypes[nargs];
  Datum values[nargs];
  int ret;
  int length = snprintf(NULL, 0, insert_template, CRON_SCHEMA_NAME, HIST_TABLE_NAME);
  char *insert = palloc((length + 1) * sizeof(char));
  snprintf(insert, length + 1, insert_template, CRON_SCHEMA_NAME, HIST_TABLE_NAME);

  memset(argtypes, 0, sizeof(argtypes));
  memset(values, 0, sizeof(values));

  argtypes[0] = INT8OID;
  argtypes[1] = TEXTOID;

  values[0] = Int64GetDatum(jobId);
  values[1] = CStringGetTextDatum(command);

  SPI_connect();
  ret = SPI_execute_with_args(insert,
      nargs, argtypes, values,
      NULL, false, 0);
  if (ret != SPI_OK_INSERT)
  {
    ereport(ERROR, (errmsg("pg_cron history record entry failed, SPI error code %d", ret)));
  }
  SPI_finish();
}

void RecordJobStarted(int64 jobId, const char *command)
{
  const char *insert_template = "INSERT INTO %s.%s (jobid, message) VALUES ($1, jsonb_build_object('event', 'started', 'command', $2));";
  int nargs = 2;
  Oid argtypes[nargs];
  Datum values[nargs];
  int ret;

  int length = snprintf(NULL, 0, insert_template, CRON_SCHEMA_NAME, HIST_TABLE_NAME);
  char *insert = palloc((length + 1) * sizeof(char));
  snprintf(insert, length + 1, insert_template, CRON_SCHEMA_NAME, HIST_TABLE_NAME);

  memset(argtypes, 0, sizeof(argtypes));
  memset(values, 0, sizeof(values));

  argtypes[0] = INT8OID;
  argtypes[1] = TEXTOID;

  values[0] = Int64GetDatum(jobId);
  values[1] = CStringGetTextDatum(command);

  SetCurrentStatementStartTimestamp();
  StartTransactionCommand();
  SPI_connect();
  PushActiveSnapshot(GetTransactionSnapshot());
  ret = SPI_execute_with_args(insert,
      nargs, argtypes, values,
      NULL, false, 0);
  if (ret != SPI_OK_INSERT)
  {
    ereport(ERROR, (errmsg("pg_cron history record entry failed, SPI error code %d", ret)));
  }
  SPI_finish();
  PopActiveSnapshot();
  CommitTransactionCommand();
}

void RecordJobCompletedStatus(int64 jobId, const char *command, const char *commandStatus, const char *tupleCount)
{
  const char *insert_template = "INSERT INTO %s.%s (jobid, message) "
                                "VALUES ($1, jsonb_build_object('event', 'completed', 'command', $2, 'command_status', $3, 'tuple_count', $4))";
  int nargs = 4;
  Oid argtypes[nargs];
  Datum values[nargs];
  int ret;

  int length = snprintf(NULL, 0, insert_template, CRON_SCHEMA_NAME, HIST_TABLE_NAME);
  char *insert = palloc((length + 1) * sizeof(char));
  snprintf(insert, length + 1, insert_template, CRON_SCHEMA_NAME, HIST_TABLE_NAME);

  memset(argtypes, 0, sizeof(argtypes));
  memset(values, 0, sizeof(values));

  argtypes[0] = INT8OID;
  argtypes[1] = TEXTOID;
  argtypes[2] = TEXTOID;
  argtypes[3] = TEXTOID;

  values[0] = Int64GetDatum(jobId);
  values[1] = CStringGetTextDatum(command);
  values[2] = CStringGetTextDatum(commandStatus);
  values[3] = CStringGetTextDatum(tupleCount);

  SetCurrentStatementStartTimestamp();
  StartTransactionCommand();
  SPI_connect();
  PushActiveSnapshot(GetTransactionSnapshot());
  ret = SPI_execute_with_args(insert,
      nargs, argtypes, values,
      NULL, false, 0);
  if (ret != SPI_OK_INSERT)
  {
    ereport(ERROR, (errmsg("pg_cron history record entry failed, SPI error code %d", ret)));
  }
  SPI_finish();
  PopActiveSnapshot();
  CommitTransactionCommand();
}


void RecordJobCompleted(int64 jobId, const char *command, int tupleCount)
{
  const char *insert_template = "INSERT INTO %s.%s (jobid, message) "
                                "VALUES ($1, jsonb_build_object('event', 'completed', 'command', $2, 'tuple_count', $3))";
  int nargs = 3;
  Oid argtypes[nargs];
  Datum values[nargs];
  int ret;
  int tupleStrLength = snprintf(NULL, 0, "%d", tupleCount);
  char *tupleStr = palloc((tupleStrLength + 1) * sizeof(char));

  int length = snprintf(NULL, 0, insert_template, CRON_SCHEMA_NAME, HIST_TABLE_NAME);
  char *insert = palloc((length + 1) * sizeof(char));
  snprintf(insert, length + 1, insert_template, CRON_SCHEMA_NAME, HIST_TABLE_NAME);
  snprintf(tupleStr, tupleStrLength + 1, "%d", tupleCount);

  memset(argtypes, 0, sizeof(argtypes));
  memset(values, 0, sizeof(values));

  argtypes[0] = INT8OID;
  argtypes[1] = TEXTOID;
  argtypes[2] = TEXTOID;


  values[0] = Int64GetDatum(jobId);
  values[1] = CStringGetTextDatum(command);
  values[2] = CStringGetTextDatum(tupleStr);

  SetCurrentStatementStartTimestamp();
  StartTransactionCommand();
  SPI_connect();
  PushActiveSnapshot(GetTransactionSnapshot());
  ret = SPI_execute_with_args(insert,
      nargs, argtypes, values,
      NULL, false, 0);
  if (ret != SPI_OK_INSERT)
  {
    ereport(ERROR, (errmsg("pg_cron history record entry failed, SPI error code %d", ret)));
  }
  SPI_finish();
  PopActiveSnapshot();
  CommitTransactionCommand();
}


void RecordJobFailed(int64 jobId, const char *command)
{
  const char *insert_template = "INSERT INTO %s.%s (jobid, message) "
                                "VALUES ($1, jsonb_build_object('event', 'failed', 'command', $2))";
  int nargs = 2;
  Oid argtypes[nargs];
  Datum values[nargs];
  int ret;

  int length = snprintf(NULL, 0, insert_template, CRON_SCHEMA_NAME, HIST_TABLE_NAME);
  char *insert = palloc((length + 1) * sizeof(char));
  snprintf(insert, length + 1, insert_template, CRON_SCHEMA_NAME, HIST_TABLE_NAME);

  memset(argtypes, 0, sizeof(argtypes));
  memset(values, 0, sizeof(values));

  argtypes[0] = INT8OID;
  argtypes[1] = TEXTOID;


  values[0] = Int64GetDatum(jobId);
  values[1] = CStringGetTextDatum(command);

  SetCurrentStatementStartTimestamp();
  StartTransactionCommand();
  SPI_connect();
  PushActiveSnapshot(GetTransactionSnapshot());
  ret = SPI_execute_with_args(insert,
      nargs, argtypes, values,
      NULL, false, 0);
  if (ret != SPI_OK_INSERT)
  {
    ereport(ERROR, (errmsg("pg_cron history record entry failed, SPI error code %d", ret)));
  }
  SPI_finish();
  PopActiveSnapshot();
  CommitTransactionCommand();
}


void RecordJobFailedMessage(int64 jobId, const char *command, const char *message)
{
  const char *insert_template = "INSERT INTO %s.%s (jobid, message) "
                                "VALUES ($1, jsonb_build_object('event', 'failed', 'command', $2, 'message', $3))";
  int nargs = 3;
  Oid argtypes[nargs];
  Datum values[nargs];
  int ret;
  int length = snprintf(NULL, 0, insert_template, CRON_SCHEMA_NAME, HIST_TABLE_NAME);
  char *insert = palloc((length + 1) * sizeof(char));
  snprintf(insert, length + 1, insert_template, CRON_SCHEMA_NAME, HIST_TABLE_NAME);

  memset(argtypes, 0, sizeof(argtypes));
  memset(values, 0, sizeof(values));

  argtypes[0] = INT8OID;
  argtypes[1] = TEXTOID;
  argtypes[2] = TEXTOID;


  values[0] = Int64GetDatum(jobId);
  values[1] = CStringGetTextDatum(command);
  values[2] = CStringGetTextDatum(message);

  SetCurrentStatementStartTimestamp();
  StartTransactionCommand();
  SPI_connect();
  PushActiveSnapshot(GetTransactionSnapshot());
  ret = SPI_execute_with_args(insert,
      nargs, argtypes, values,
      NULL, false, 0);
  if (ret != SPI_OK_INSERT)
  {
    ereport(ERROR, (errmsg("pg_cron history record entry failed, SPI error code %d", ret)));
  }
  SPI_finish();
  PopActiveSnapshot();
  CommitTransactionCommand();
}
