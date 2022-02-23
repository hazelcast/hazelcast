/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.jet.impl.JobExecutionRecord;
import com.hazelcast.jet.impl.JobExecutionRecord.SnapshotStats;
import com.hazelcast.jet.impl.JobRecord;
import com.hazelcast.jet.impl.JobRepository.FilterJobResultByNamePredicate;
import com.hazelcast.jet.impl.JobRepository.UpdateJobExecutionRecordEntryProcessor;
import com.hazelcast.jet.impl.JobResult;
import com.hazelcast.jet.impl.JobSummary;
import com.hazelcast.jet.impl.JobSuspensionCauseImpl;
import com.hazelcast.jet.impl.SnapshotValidationRecord;
import com.hazelcast.jet.impl.SqlSummary;
import com.hazelcast.jet.impl.connector.WriteFileP;
import com.hazelcast.jet.impl.operation.CheckLightJobsOperation;
import com.hazelcast.jet.impl.operation.GetJobConfigOperation;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation.GetJobIdsResult;
import com.hazelcast.jet.impl.operation.GetJobMetricsOperation;
import com.hazelcast.jet.impl.operation.GetJobStatusOperation;
import com.hazelcast.jet.impl.operation.GetJobSubmissionTimeOperation;
import com.hazelcast.jet.impl.operation.GetJobSummaryListOperation;
import com.hazelcast.jet.impl.operation.GetJobSuspensionCauseOperation;
import com.hazelcast.jet.impl.operation.GetLocalJobMetricsOperation;
import com.hazelcast.jet.impl.operation.InitExecutionOperation;
import com.hazelcast.jet.impl.operation.JoinSubmittedJobOperation;
import com.hazelcast.jet.impl.operation.NotifyMemberShutdownOperation;
import com.hazelcast.jet.impl.operation.PrepareForPassiveClusterOperation;
import com.hazelcast.jet.impl.operation.ResumeJobOperation;
import com.hazelcast.jet.impl.operation.SnapshotPhase1Operation;
import com.hazelcast.jet.impl.operation.SnapshotPhase1Operation.SnapshotPhase1Result;
import com.hazelcast.jet.impl.operation.SnapshotPhase2Operation;
import com.hazelcast.jet.impl.operation.StartExecutionOperation;
import com.hazelcast.jet.impl.operation.SubmitJobOperation;
import com.hazelcast.jet.impl.operation.TerminateExecutionOperation;
import com.hazelcast.jet.impl.operation.TerminateJobOperation;
import com.hazelcast.jet.impl.processor.NoopP;
import com.hazelcast.jet.impl.processor.ProcessorSupplierFromSimpleSupplier;
import com.hazelcast.jet.impl.processor.SessionWindowP;
import com.hazelcast.jet.impl.processor.SlidingWindowP.SnapshotKey;
import com.hazelcast.jet.impl.util.AsyncSnapshotWriterImpl;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_IMPL_DS_FACTORY;
import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_IMPL_DS_FACTORY_ID;

public final class JetInitDataSerializerHook implements DataSerializerHook {

    public static final int EXECUTION_PLAN = 0;
    public static final int VERTEX_DEF = 1;
    public static final int EDGE_DEF = 2;
    public static final int JOB_RECORD = 3;
    public static final int JOB_RESULT = 4;
    public static final int INIT_EXECUTION_OP = 5;
    public static final int START_EXECUTION_OP = 6;
    public static final int SUBMIT_JOB_OP = 8;
    public static final int GET_JOB_STATUS_OP = 9;
    public static final int SNAPSHOT_PHASE1_OPERATION = 10;
    public static final int JOB_EXECUTION_RECORD = 11;
    public static final int SESSION_WINDOW_P_WINDOWS = 12;
    public static final int SLIDING_WINDOW_P_SNAPSHOT_KEY = 13;
    public static final int GET_JOB_IDS = 14;
    public static final int JOIN_SUBMITTED_JOB = 15;
    public static final int UPDATE_JOB_EXECUTION_RECORD_EP = 16;
    public static final int TERMINATE_EXECUTION_OP = 17;
    public static final int FILTER_JOB_RESULT_BY_NAME = 18;
    public static final int GET_JOB_IDS_RESULT = 19;
    public static final int GET_JOB_SUBMISSION_TIME_OP = 20;
    public static final int GET_JOB_CONFIG_OP = 21;
    public static final int TERMINATE_JOB_OP = 22;
    public static final int ASYNC_SNAPSHOT_WRITER_SNAPSHOT_DATA_KEY = 23;
    public static final int ASYNC_SNAPSHOT_WRITER_SNAPSHOT_DATA_VALUE_TERMINATOR = 24;
    public static final int SNAPSHOT_PHASE1_RESULT = 25;
    public static final int RESUME_JOB_OP = 26;
    public static final int NOTIFY_MEMBER_SHUTDOWN_OP = 27;
    public static final int GET_JOB_SUMMARY_LIST_OP = 28;
    public static final int JOB_SUMMARY = 29;
    public static final int SNAPSHOT_STATS = 30;
    public static final int PREPARE_FOR_PASSIVE_CLUSTER_OP = 31;
    public static final int SNAPSHOT_VALIDATION_RECORD = 32;
    // Moved to AggregateDataSerializerHook
    // public static final int AGGREGATE_OP_AGGREGATOR = 33;
    public static final int GET_JOB_METRICS_OP = 34;
    public static final int GET_LOCAL_JOB_METRICS_OP = 35;
    public static final int SNAPSHOT_PHASE2_OPERATION = 36;
    public static final int WRITE_FILE_P_FILE_ID = 42;
    public static final int JOB_SUSPENSION_CAUSE = 43;
    public static final int GET_JOB_SUSPENSION_CAUSE_OP = 44;
    public static final int PROCESSOR_SUPPLIER_FROM_SIMPLE_SUPPLIER = 45;
    public static final int NOOP_PROCESSOR_SUPPLIER = 46;
    public static final int CHECK_LIGHT_JOBS_OP = 47;
    public static final int SQL_SUMMARY = 48;

    public static final int FACTORY_ID = FactoryIdHelper.getFactoryId(JET_IMPL_DS_FACTORY, JET_IMPL_DS_FACTORY_ID);

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new Factory();
    }

    private static class Factory implements DataSerializableFactory {
        @SuppressWarnings("checkstyle:returncount")
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            switch (typeId) {
                case EXECUTION_PLAN:
                    return new ExecutionPlan();
                case EDGE_DEF:
                    return new EdgeDef();
                case VERTEX_DEF:
                    return new VertexDef();
                case JOB_RECORD:
                    return new JobRecord();
                case JOB_RESULT:
                    return new JobResult();
                case INIT_EXECUTION_OP:
                    return new InitExecutionOperation();
                case START_EXECUTION_OP:
                    return new StartExecutionOperation();
                case SUBMIT_JOB_OP:
                    return new SubmitJobOperation();
                case GET_JOB_STATUS_OP:
                    return new GetJobStatusOperation();
                case SNAPSHOT_PHASE1_OPERATION:
                    return new SnapshotPhase1Operation();
                case JOB_EXECUTION_RECORD:
                    return new JobExecutionRecord();
                case SESSION_WINDOW_P_WINDOWS:
                    return new SessionWindowP.Windows<>();
                case SLIDING_WINDOW_P_SNAPSHOT_KEY:
                    return new SnapshotKey();
                case GET_JOB_IDS:
                    return new GetJobIdsOperation();
                case JOIN_SUBMITTED_JOB:
                    return new JoinSubmittedJobOperation();
                case UPDATE_JOB_EXECUTION_RECORD_EP:
                    return new UpdateJobExecutionRecordEntryProcessor();
                case TERMINATE_EXECUTION_OP:
                    return new TerminateExecutionOperation();
                case FILTER_JOB_RESULT_BY_NAME:
                    return new FilterJobResultByNamePredicate();
                case GET_JOB_IDS_RESULT:
                    return new GetJobIdsResult();
                case GET_JOB_SUBMISSION_TIME_OP:
                    return new GetJobSubmissionTimeOperation();
                case GET_JOB_CONFIG_OP:
                    return new GetJobConfigOperation();
                case TERMINATE_JOB_OP:
                    return new TerminateJobOperation();
                case ASYNC_SNAPSHOT_WRITER_SNAPSHOT_DATA_KEY:
                    return new AsyncSnapshotWriterImpl.SnapshotDataKey();
                case ASYNC_SNAPSHOT_WRITER_SNAPSHOT_DATA_VALUE_TERMINATOR:
                    return AsyncSnapshotWriterImpl.SnapshotDataValueTerminator.INSTANCE;
                case SNAPSHOT_PHASE1_RESULT:
                    return new SnapshotPhase1Result();
                case RESUME_JOB_OP:
                    return new ResumeJobOperation();
                case NOTIFY_MEMBER_SHUTDOWN_OP:
                    return new NotifyMemberShutdownOperation();
                case GET_JOB_SUMMARY_LIST_OP:
                    return new GetJobSummaryListOperation();
                case JOB_SUMMARY:
                    return new JobSummary();
                case SNAPSHOT_STATS:
                    return new SnapshotStats();
                case PREPARE_FOR_PASSIVE_CLUSTER_OP:
                    return new PrepareForPassiveClusterOperation();
                case SNAPSHOT_VALIDATION_RECORD:
                    return new SnapshotValidationRecord();
                case GET_JOB_METRICS_OP:
                    return new GetJobMetricsOperation();
                case GET_LOCAL_JOB_METRICS_OP:
                    return new GetLocalJobMetricsOperation();
                case SNAPSHOT_PHASE2_OPERATION:
                    return new SnapshotPhase2Operation();
                case WRITE_FILE_P_FILE_ID:
                    return new WriteFileP.FileId();
                case JOB_SUSPENSION_CAUSE:
                    return new JobSuspensionCauseImpl();
                case GET_JOB_SUSPENSION_CAUSE_OP:
                    return new GetJobSuspensionCauseOperation();
                case PROCESSOR_SUPPLIER_FROM_SIMPLE_SUPPLIER:
                    return new ProcessorSupplierFromSimpleSupplier();
                case NOOP_PROCESSOR_SUPPLIER:
                    return new NoopP.NoopPSupplier();
                case CHECK_LIGHT_JOBS_OP:
                    return new CheckLightJobsOperation();
                case SQL_SUMMARY:
                    return new SqlSummary();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
