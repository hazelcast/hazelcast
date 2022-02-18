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

package com.hazelcast.durableexecutor.impl;

import com.hazelcast.durableexecutor.impl.operations.DisposeResultBackupOperation;
import com.hazelcast.durableexecutor.impl.operations.DisposeResultOperation;
import com.hazelcast.durableexecutor.impl.operations.PutResultBackupOperation;
import com.hazelcast.durableexecutor.impl.operations.PutResultOperation;
import com.hazelcast.durableexecutor.impl.operations.ReplicationOperation;
import com.hazelcast.durableexecutor.impl.operations.RetrieveAndDisposeResultOperation;
import com.hazelcast.durableexecutor.impl.operations.RetrieveResultOperation;
import com.hazelcast.durableexecutor.impl.operations.ShutdownOperation;
import com.hazelcast.durableexecutor.impl.operations.TaskBackupOperation;
import com.hazelcast.durableexecutor.impl.operations.TaskOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.DURABLE_EXECUTOR_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.DURABLE_EXECUTOR_DS_FACTORY_ID;

public class DurableExecutorDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(DURABLE_EXECUTOR_DS_FACTORY, DURABLE_EXECUTOR_DS_FACTORY_ID);

    public static final int DISPOSE_RESULT_BACKUP = 0;
    public static final int DISPOSE_RESULT = 1;
    public static final int PUT_RESULT = 2;
    public static final int REPLICATION = 3;
    public static final int RETRIEVE_DISPOSE_RESULT = 4;
    public static final int RETRIEVE_RESULT = 5;
    public static final int SHUTDOWN = 6;
    public static final int TASK_BACKUP = 7;
    public static final int TASK = 8;
    public static final int PUT_RESULT_BACKUP = 9;

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
                    case DISPOSE_RESULT_BACKUP:
                        return new DisposeResultBackupOperation();
                    case DISPOSE_RESULT:
                        return new DisposeResultOperation();
                    case PUT_RESULT:
                        return new PutResultOperation();
                    case PUT_RESULT_BACKUP:
                        return new PutResultBackupOperation();
                    case REPLICATION:
                        return new ReplicationOperation();
                    case RETRIEVE_DISPOSE_RESULT:
                        return new RetrieveAndDisposeResultOperation();
                    case RETRIEVE_RESULT:
                        return new RetrieveResultOperation();
                    case SHUTDOWN:
                        return new ShutdownOperation();
                    case TASK_BACKUP:
                        return new TaskBackupOperation();
                    case TASK:
                        return new TaskOperation();
                    default:
                        return null;
                }
            }
        };
    }
}
