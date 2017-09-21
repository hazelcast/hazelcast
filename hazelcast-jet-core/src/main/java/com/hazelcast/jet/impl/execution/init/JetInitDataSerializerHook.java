/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.impl.JobRecord;
import com.hazelcast.jet.impl.JobRepository.FilterExecutionIdByJobIdPredicate;
import com.hazelcast.jet.impl.JobRepository.FilterJobIdPredicate;
import com.hazelcast.jet.impl.JobResult;
import com.hazelcast.jet.impl.execution.SnapshotRecord;
import com.hazelcast.jet.impl.operation.CompleteOperation;
import com.hazelcast.jet.impl.operation.ExecuteOperation;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation;
import com.hazelcast.jet.impl.operation.GetJobStatusOperation;
import com.hazelcast.jet.impl.operation.InitOperation;
import com.hazelcast.jet.impl.operation.JoinSubmittedJobOperation;
import com.hazelcast.jet.impl.operation.SnapshotOperation;
import com.hazelcast.jet.impl.operation.SubmitJobOperation;
import com.hazelcast.jet.impl.processor.SessionWindowP;
import com.hazelcast.jet.impl.processor.SnapshotKey;
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
    public static final int INIT_OP = 5;
    public static final int EXECUTE_OP = 6;
    public static final int COMPLETE_OP = 7;
    public static final int SUBMIT_JOB_OP = 8;
    public static final int GET_JOB_STATUS_OP = 9;
    public static final int SNAPSHOT_OP = 10;
    public static final int MASTER_SNAPSHOT_RECORD = 11;
    public static final int SESSION_WINDOW_P_WINDOWS = 12;
    public static final int FILTER_EXECUTION_ID_BY_JOB_ID_PREDICATE = 13;
    public static final int FILTER_JOB_ID = 14;
    public static final int SLIDING_WINDOW_P_SNAPSHOT_KEY = 15;
    public static final int GET_JOB_IDS = 16;
    public static final int JOIN_SUBMITTED_JOB = 17;

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
                case INIT_OP:
                    return new InitOperation();
                case EXECUTE_OP:
                    return new ExecuteOperation();
                case COMPLETE_OP:
                    return new CompleteOperation();
                case SUBMIT_JOB_OP:
                    return new SubmitJobOperation();
                case GET_JOB_STATUS_OP:
                    return new GetJobStatusOperation();
                case SNAPSHOT_OP:
                    return new SnapshotOperation();
                case MASTER_SNAPSHOT_RECORD:
                    return new SnapshotRecord();
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
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
