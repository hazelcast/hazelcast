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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.scheduledexecutor.impl.operations.CancelTaskBackupOperation;
import com.hazelcast.scheduledexecutor.impl.operations.CancelTaskOperation;
import com.hazelcast.scheduledexecutor.impl.operations.DisposeBackupTaskOperation;
import com.hazelcast.scheduledexecutor.impl.operations.DisposeTaskOperation;
import com.hazelcast.scheduledexecutor.impl.operations.GetAllScheduledOnMemberOperation;
import com.hazelcast.scheduledexecutor.impl.operations.GetAllScheduledOnPartitionOperation;
import com.hazelcast.scheduledexecutor.impl.operations.GetAllScheduledOnPartitionOperationFactory;
import com.hazelcast.scheduledexecutor.impl.operations.GetDelayOperation;
import com.hazelcast.scheduledexecutor.impl.operations.GetResultOperation;
import com.hazelcast.scheduledexecutor.impl.operations.GetStatisticsOperation;
import com.hazelcast.scheduledexecutor.impl.operations.IsCanceledOperation;
import com.hazelcast.scheduledexecutor.impl.operations.IsDoneOperation;
import com.hazelcast.scheduledexecutor.impl.operations.MergeBackupOperation;
import com.hazelcast.scheduledexecutor.impl.operations.MergeOperation;
import com.hazelcast.scheduledexecutor.impl.operations.ReplicationOperation;
import com.hazelcast.scheduledexecutor.impl.operations.ResultReadyNotifyOperation;
import com.hazelcast.scheduledexecutor.impl.operations.ScheduleTaskBackupOperation;
import com.hazelcast.scheduledexecutor.impl.operations.ScheduleTaskOperation;
import com.hazelcast.scheduledexecutor.impl.operations.ShutdownOperation;
import com.hazelcast.scheduledexecutor.impl.operations.SyncBackupStateOperation;
import com.hazelcast.scheduledexecutor.impl.operations.SyncStateOperation;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SCHEDULED_EXECUTOR_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SCHEDULED_EXECUTOR_DS_FACTORY_ID;

public class ScheduledExecutorDataSerializerHook
        implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(SCHEDULED_EXECUTOR_DS_FACTORY, SCHEDULED_EXECUTOR_DS_FACTORY_ID);

    public static final int TASK_HANDLER = 1;
    public static final int TASK_DESCRIPTOR = 2;
    public static final int RUNNABLE_DEFINITION = 3;

    public static final int RUNNABLE_ADAPTER = 4;
    public static final int NAMED_TASK_DECORATOR = 5;

    public static final int SCHEDULE_OP = 6;
    public static final int SCHEDULE_BACKUP_OP = 7;

    public static final int CANCEL_OP = 8;
    public static final int CANCEL_BACKUP_OP = 9;

    public static final int GET_RESULT = 10;
    public static final int PUBLISH_RESULT = 11;
    public static final int GET_DELAY_OP = 12;
    public static final int IS_DONE_OP = 13;
    public static final int IS_CANCELED_OP = 14;
    public static final int GET_STATS_OP = 15;
    public static final int TASK_STATS = 16;

    public static final int SYNC_STATE_OP = 17;
    public static final int SYNC_BACKUP_STATE_OP = 18;
    public static final int REPLICATION = 19;

    public static final int DISPOSE_TASK_OP = 20;

    public static final int DISPOSE_BACKUP_TASK_OP = 21;

    public static final int GET_ALL_SCHEDULED_ON_MEMBER = 22;

    public static final int GET_ALL_SCHEDULED_ON_PARTITION = 25;

    public static final int GET_ALL_SCHEDULED_ON_PARTITION_OPERATION_FACTORY = 26;

    public static final int SHUTDOWN = 23;

    public static final int TASK_RESOLUTION = 24;

    public static final int MERGE = 27;

    public static final int MERGE_BACKUP = 28;

    public static final int HASH_MAP_ADAPTER = 29;

    public static final int AUTO_DISPOSABLE_TASK_DECORATOR = 30;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case TASK_HANDLER:
                        return new ScheduledTaskHandlerImpl();
                    case TASK_DESCRIPTOR:
                        return new ScheduledTaskDescriptor();
                    case RUNNABLE_DEFINITION:
                        return new TaskDefinition();
                    case RUNNABLE_ADAPTER:
                        return new ScheduledRunnableAdapter();
                    case NAMED_TASK_DECORATOR:
                        return new NamedTaskDecorator();
                    case GET_DELAY_OP:
                        return new GetDelayOperation();
                    case CANCEL_OP:
                        return new CancelTaskOperation();
                    case CANCEL_BACKUP_OP:
                        return new CancelTaskBackupOperation();
                    case SCHEDULE_OP:
                        return new ScheduleTaskOperation();
                    case DISPOSE_TASK_OP:
                        return new DisposeTaskOperation();
                    case DISPOSE_BACKUP_TASK_OP:
                        return new DisposeBackupTaskOperation();
                    case IS_DONE_OP:
                        return new IsDoneOperation();
                    case IS_CANCELED_OP:
                        return new IsCanceledOperation();
                    case TASK_STATS:
                        return new ScheduledTaskStatisticsImpl();
                    case GET_STATS_OP:
                        return new GetStatisticsOperation();
                    case SCHEDULE_BACKUP_OP:
                        return new ScheduleTaskBackupOperation();
                    case SYNC_STATE_OP:
                        return new SyncStateOperation();
                    case SYNC_BACKUP_STATE_OP:
                        return new SyncBackupStateOperation();
                    case REPLICATION:
                        return new ReplicationOperation();
                    case GET_ALL_SCHEDULED_ON_MEMBER:
                        return new GetAllScheduledOnMemberOperation();
                    case GET_RESULT:
                        return new GetResultOperation();
                    case PUBLISH_RESULT:
                        return new ResultReadyNotifyOperation();
                    case SHUTDOWN:
                        return new ShutdownOperation();
                    case TASK_RESOLUTION:
                        return new ScheduledTaskResult();
                    case GET_ALL_SCHEDULED_ON_PARTITION:
                        return new GetAllScheduledOnPartitionOperation();
                    case GET_ALL_SCHEDULED_ON_PARTITION_OPERATION_FACTORY:
                        return new GetAllScheduledOnPartitionOperationFactory();
                    case MERGE:
                        return new MergeOperation();
                    case MERGE_BACKUP:
                        return new MergeBackupOperation();
                    case HASH_MAP_ADAPTER:
                        return new HashMapAdapter<>();
                    case AUTO_DISPOSABLE_TASK_DECORATOR:
                        return new AutoDisposableTaskDecorator();
                    default:
                        throw new IllegalArgumentException("Illegal Scheduled Executor serializer type ID: " + typeId);
                }
            }
        };
    }
}
