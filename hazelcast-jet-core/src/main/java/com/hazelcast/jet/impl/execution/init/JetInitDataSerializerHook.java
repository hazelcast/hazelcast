/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.impl.ClusterMetadata;
import com.hazelcast.jet.impl.JobExecutionRecord;
import com.hazelcast.jet.impl.JobExecutionRecord.SnapshotStats;
import com.hazelcast.jet.impl.JobRecord;
import com.hazelcast.jet.impl.JobRepository.FilterExecutionIdByJobIdPredicate;
import com.hazelcast.jet.impl.JobRepository.FilterJobIdPredicate;
import com.hazelcast.jet.impl.JobRepository.FilterJobRecordByNamePredicate;
import com.hazelcast.jet.impl.JobRepository.FilterJobResultByNamePredicate;
import com.hazelcast.jet.impl.JobRepository.UpdateJobExecutionRecordEntryProcessor;
import com.hazelcast.jet.impl.JobResult;
import com.hazelcast.jet.impl.JobSummary;
import com.hazelcast.jet.impl.SnapshotValidationRecord;
import com.hazelcast.jet.impl.operation.CompleteExecutionOperation;
import com.hazelcast.jet.impl.operation.ExportSnapshotOperation;
import com.hazelcast.jet.impl.operation.GetClusterMetadataOperation;
import com.hazelcast.jet.impl.operation.GetJobConfigOperation;
import com.hazelcast.jet.impl.operation.GetJobIdsByNameOperation;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation;
import com.hazelcast.jet.impl.operation.GetJobStatusOperation;
import com.hazelcast.jet.impl.operation.GetJobSubmissionTimeOperation;
import com.hazelcast.jet.impl.operation.GetJobSummaryListOperation;
import com.hazelcast.jet.impl.operation.InitExecutionOperation;
import com.hazelcast.jet.impl.operation.JoinSubmittedJobOperation;
import com.hazelcast.jet.impl.operation.NotifyMemberShutdownOperation;
import com.hazelcast.jet.impl.operation.PrepareForPassiveClusterOperation;
import com.hazelcast.jet.impl.operation.ResumeJobOperation;
import com.hazelcast.jet.impl.operation.SnapshotOperation;
import com.hazelcast.jet.impl.operation.SnapshotOperation.SnapshotOperationResult;
import com.hazelcast.jet.impl.operation.StartExecutionOperation;
import com.hazelcast.jet.impl.operation.SubmitJobOperation;
import com.hazelcast.jet.impl.operation.TerminateExecutionOperation;
import com.hazelcast.jet.impl.operation.TerminateJobOperation;
import com.hazelcast.jet.impl.processor.SessionWindowP;
import com.hazelcast.jet.impl.processor.SlidingWindowP.SnapshotKey;
import com.hazelcast.jet.impl.util.AsyncSnapshotWriterImpl;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public final class JetInitDataSerializerHook implements DataSerializerHook {

    public static final String JET_IMPL_DS_FACTORY = "hazelcast.serialization.ds.jet.impl";
    public static final int JET_IMPL_DS_FACTORY_ID = -10002;

    public static final int EXECUTION_PLAN = 0;
    public static final int VERTEX_DEF = 1;
    public static final int EDGE_DEF = 2;
    public static final int JOB_RECORD = 3;
    public static final int JOB_RESULT = 4;
    public static final int INIT_EXECUTION_OP = 5;
    public static final int START_EXECUTION_OP = 6;
    public static final int COMPLETE_EXECUTION_OP = 7;
    public static final int SUBMIT_JOB_OP = 8;
    public static final int GET_JOB_STATUS_OP = 9;
    public static final int SNAPSHOT_OPERATION = 10;
    public static final int JOB_EXECUTION_RECORD = 11;
    public static final int SESSION_WINDOW_P_WINDOWS = 12;
    public static final int FILTER_EXECUTION_ID_BY_JOB_ID_PREDICATE = 13;
    public static final int FILTER_JOB_ID = 14;
    public static final int SLIDING_WINDOW_P_SNAPSHOT_KEY = 15;
    public static final int GET_JOB_IDS = 16;
    public static final int JOIN_SUBMITTED_JOB = 17;
    public static final int UPDATE_JOB_EXECUTION_RECORD_EP = 18;
    public static final int TERMINATE_EXECUTION_OP = 20;
    public static final int FILTER_JOB_RECORD_BY_NAME = 21;
    public static final int FILTER_JOB_RESULT_BY_NAME = 22;
    public static final int GET_JOB_IDS_BY_NAME_OP = 23;
    public static final int GET_JOB_SUBMISSION_TIME_OP = 24;
    public static final int GET_JOB_CONFIG_OP = 25;
    public static final int TERMINATE_JOB_OP = 26;
    public static final int ASYNC_SNAPSHOT_WRITER_SNAPSHOT_DATA_KEY = 27;
    public static final int ASYNC_SNAPSHOT_WRITER_SNAPSHOT_DATA_VALUE_TERMINATOR = 28;
    public static final int SNAPSHOT_OPERATION_RESULT = 29;
    public static final int RESUME_JOB_OP = 30;
    public static final int NOTIFY_MEMBER_SHUTDOWN_OP = 31;
    public static final int GET_JOB_SUMMARY_LIST_OP = 32;
    public static final int JOB_SUMMARY = 33;
    public static final int SNAPSHOT_STATS = 34;
    public static final int PREPARE_FOR_PASSIVE_CLUSTER_OP = 35;
    public static final int EXPORT_SNAPSHOT_OP = 36;
    public static final int SNAPSHOT_VALIDATION_RECORD = 37;
    public static final int CLUSTER_METADATA = 38;
    public static final int GET_CLUSTER_METADATA_OP = 39;

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
                case COMPLETE_EXECUTION_OP:
                    return new CompleteExecutionOperation();
                case SUBMIT_JOB_OP:
                    return new SubmitJobOperation();
                case GET_JOB_STATUS_OP:
                    return new GetJobStatusOperation();
                case SNAPSHOT_OPERATION:
                    return new SnapshotOperation();
                case JOB_EXECUTION_RECORD:
                    return new JobExecutionRecord();
                case SESSION_WINDOW_P_WINDOWS:
                    return new SessionWindowP.Windows<>();
                case FILTER_EXECUTION_ID_BY_JOB_ID_PREDICATE:
                    return new FilterExecutionIdByJobIdPredicate();
                case FILTER_JOB_ID:
                    return new FilterJobIdPredicate();
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
                case FILTER_JOB_RECORD_BY_NAME:
                    return new FilterJobRecordByNamePredicate();
                case FILTER_JOB_RESULT_BY_NAME:
                    return new FilterJobResultByNamePredicate();
                case GET_JOB_IDS_BY_NAME_OP:
                    return new GetJobIdsByNameOperation();
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
                case SNAPSHOT_OPERATION_RESULT:
                    return new SnapshotOperationResult();
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
                case EXPORT_SNAPSHOT_OP:
                    return new ExportSnapshotOperation();
                case SNAPSHOT_VALIDATION_RECORD:
                    return new SnapshotValidationRecord();
                case CLUSTER_METADATA:
                    return new ClusterMetadata();
                case GET_CLUSTER_METADATA_OP:
                    return new GetClusterMetadataOperation();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
