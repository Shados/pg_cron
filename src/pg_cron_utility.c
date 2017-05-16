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

#define HIST_TABLE_NAME "history"
#define HIST_ID_SEQUENCE_NAME "cron.histid_seq"

/*
 * CronExtensionOwner returns the name of the user that owns the
 * extension.
 */
Oid
CronExtensionOwner(void)
{
	Relation extensionRelation = NULL;
	SysScanDesc scanDescriptor;
	ScanKeyData entry[1];
	HeapTuple extensionTuple = NULL;
	Form_pg_extension extensionForm = NULL;
	Oid extensionOwner = InvalidOid;

	extensionRelation = heap_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0],
				Anum_pg_extension_extname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(EXTENSION_NAME));

	scanDescriptor = systable_beginscan(extensionRelation, ExtensionNameIndexId,
										true, NULL, 1, entry);

	extensionTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(extensionTuple))
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("pg_cron extension not loaded")));
	}

	extensionForm = (Form_pg_extension) GETSTRUCT(extensionTuple);
	extensionOwner = extensionForm->extowner;

	systable_endscan(scanDescriptor);
	heap_close(extensionRelation, AccessShareLock);

	return extensionOwner;
}


/*
 * NextSeqId returns a new, unique ID using the specified sequence.
 */
int64
NextSeqId(const char *sequence)
{
	text *sequenceName = NULL;
	Oid sequenceId = InvalidOid;
	List *sequenceNameList = NIL;
	RangeVar *sequenceVar = NULL;
	Datum sequenceIdDatum = InvalidOid;
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	Datum seqIdDatum = 0;
	int64 seqId = 0;
	bool failOK = true;

	/* resolve relationId from passed in schema and relation name */
	sequenceName = cstring_to_text(sequence);
	sequenceNameList = textToQualifiedNameList(sequenceName);
	sequenceVar = makeRangeVarFromNameList(sequenceNameList);
	sequenceId = RangeVarGetRelid(sequenceVar, NoLock, failOK);
	sequenceIdDatum = ObjectIdGetDatum(sequenceId);

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CronExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	/* generate new and unique colocation id from sequence */
	seqIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	seqId = DatumGetUInt32(seqIdDatum);

	return seqId;
}

/*
 * AddValues adds a prepared set of Datum values as a tuple to the specified cron relation.
 */
void
AddValues(const char *rel, Datum *values, bool *isNulls)
{
	Oid cronSchemaId = InvalidOid;
	Oid relationId = InvalidOid;

	Relation table = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;

  cronSchemaId = get_namespace_oid(CRON_SCHEMA_NAME, false);
  relationId = get_relname_relid(rel, cronSchemaId);

  /* open history relation and insert new tuple */
  table = heap_open(relationId, RowExclusiveLock);

  tupleDescriptor = RelationGetDescr(table);
  heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

  simple_heap_insert(table, heapTuple);
  CatalogUpdateIndexes(table, heapTuple);
  CommandCounterIncrement();

  /* close relation and invalidate previous cache entry */
  heap_close(table, RowExclusiveLock);
  return;
}

/*
 * AddJobHistory adds a cron.history entry for the specified job with the specified message.
 */
void
AddJobHistory(int64 jobId, char *message)
{
  TimestampTz currentTime = 0;

  int64 histId = 0;
  Datum histIdDatum = 0;
  Datum values[Natts_cron_hist];
  bool isNulls[Natts_cron_hist];


  /* Prepare history entry values */
  memset(values, 0, sizeof(values));
  memset(isNulls, false, sizeof(isNulls));

  currentTime = GetCurrentTimestamp();

  histId = NextSeqId(HIST_ID_SEQUENCE_NAME);
  histIdDatum = Int64GetDatum(histId);

  values[Anum_cron_hist_histid - 1] = histIdDatum;
  values[Anum_cron_hist_jobid - 1] = Int64GetDatum(jobId);
  values[Anum_cron_hist_message - 1] = CStringGetTextDatum(message);
  values[Anum_cron_hist_created_at - 1] = TimestampTzGetDatum(currentTime);

  /* Insert values to history relation */
  AddValues(HIST_TABLE_NAME, values, isNulls);
}
