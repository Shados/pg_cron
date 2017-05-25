/*-------------------------------------------------------------------------
 *
 * job_metadata.h
 *	  definition of job metadata functions
 *
 * Copyright (c) 2010-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef JOB_METADATA_H
#define JOB_METADATA_H


#include "nodes/pg_list.h"
#include "utils/timestamp.h"


/* job metadata data structure */
typedef struct CronJob
{
	int64 jobId;
	char *scheduleText;
	entry schedule;
	char *command;
	char *nodeName;
	int nodePort;
	char *database;
	char *userName;
} CronJob;

typedef struct AtJob
{
	int64 jobId;
	TimestampTz timestamp;
	char *command;
	char *nodeName;
	int nodePort;
	char *database;
	char *userName;
} AtJob;


/* global settings */
extern bool CronJobCacheValid;
extern bool AtJobCacheValid;


/* functions for retrieving job metadata */
extern void InitializeCronJobMetadataCache(void);
extern void InitializeAtJobMetadataCache(void);
extern void ResetCronJobMetadataCache(void);
extern void ResetAtJobMetadataCache(void);
extern List * LoadCronJobList(void);
extern List * LoadAtJobList(void);
extern CronJob * GetCronJob(int64 jobId);
extern AtJob * GetAtJob(int64 jobId);
extern void RemoveAtJob(int64 jobId);


#endif
