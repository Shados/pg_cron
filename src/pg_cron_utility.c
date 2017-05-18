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
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "commands/sequence.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/snapmgr.h"

#define HIST_TABLE_NAME "history"
#define HIST_ID_SEQUENCE_NAME "cron.histid_seq"


/*
 * AddJobHistory adds a cron.history entry for the specified job with the specified message.
 */
void
AddJobHistory(int64 jobId, const char *message, bool newTransaction)
{
  StringInfoData command;
  int ret;

  if (newTransaction)
  {
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());
  }
  else
  {
    SPI_connect();
  }

  initStringInfo(&command);
  appendStringInfo(&command,
      "INSERT INTO %s.%s (jobid, message) "
      "VALUES (%ld, '%s');",
      CRON_SCHEMA_NAME, HIST_TABLE_NAME,
      jobId, message);

  ret = SPI_execute(command.data, false, 0);
  if (ret != SPI_OK_INSERT)
  {
    ereport(ERROR, (errmsg("pg_cron history record entry failed, SPI error code %d", ret)));
  }
  SPI_finish();
  if (newTransaction)
  {
    PopActiveSnapshot();
    CommitTransactionCommand();
  }
}
