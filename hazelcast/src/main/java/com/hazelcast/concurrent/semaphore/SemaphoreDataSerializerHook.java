/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.semaphore;

import com.hazelcast.concurrent.semaphore.operations.AcquireBackupOperation;
import com.hazelcast.concurrent.semaphore.operations.AcquireOperation;
import com.hazelcast.concurrent.semaphore.operations.AvailableOperation;
import com.hazelcast.concurrent.semaphore.operations.DeadMemberBackupOperation;
import com.hazelcast.concurrent.semaphore.operations.DrainBackupOperation;
import com.hazelcast.concurrent.semaphore.operations.DrainOperation;
import com.hazelcast.concurrent.semaphore.operations.InitBackupOperation;
import com.hazelcast.concurrent.semaphore.operations.InitOperation;
import com.hazelcast.concurrent.semaphore.operations.ReduceBackupOperation;
import com.hazelcast.concurrent.semaphore.operations.ReduceOperation;
import com.hazelcast.concurrent.semaphore.operations.ReleaseBackupOperation;
import com.hazelcast.concurrent.semaphore.operations.ReleaseOperation;
import com.hazelcast.concurrent.semaphore.operations.SemaphoreDeadMemberOperation;
import com.hazelcast.concurrent.semaphore.operations.SemaphoreReplicationOperation;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class SemaphoreDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.SEMAPHORE_DS_FACTORY, -16);

    public static final int ACQUIRE_BACKUP_OPERATION = 0;
    public static final int ACQUIRE_OPERATION = 1;
    public static final int AVAILABLE_OPERATION = 2;
    public static final int DEAD_MEMBER_BACKUP_OPERATION = 3;
    public static final int DRAIN_BACKUP_OPERATION = 4;
    public static final int DRAIN_OPERATION = 5;
    public static final int INIT_BACKUP_OPERATION = 6;
    public static final int INIT_OPERATION = 7;
    public static final int REDUCE_BACKUP_OPERATION = 8;
    public static final int REDUCE_OPERATION = 9;
    public static final int RELEASE_BACKUP_OPERATION = 10;
    public static final int RELEASE_OPERATION = 11;
    public static final int SEMAPHORE_DEAD_MEMBER_OPERATION = 12;
    public static final int SEMAPHORE_REPLICATION_OPERATION = 13;

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
                    case ACQUIRE_BACKUP_OPERATION:
                        return new AcquireBackupOperation();
                    case ACQUIRE_OPERATION:
                        return new AcquireOperation();
                    case AVAILABLE_OPERATION:
                        return new AvailableOperation();
                    case DEAD_MEMBER_BACKUP_OPERATION:
                        return new DeadMemberBackupOperation();
                    case DRAIN_BACKUP_OPERATION:
                        return new DrainBackupOperation();
                    case DRAIN_OPERATION:
                        return new DrainOperation();
                    case INIT_BACKUP_OPERATION:
                        return new InitBackupOperation();
                    case INIT_OPERATION:
                        return new InitOperation();
                    case REDUCE_BACKUP_OPERATION:
                        return new ReduceBackupOperation();
                    case REDUCE_OPERATION:
                        return new ReduceOperation();
                    case RELEASE_BACKUP_OPERATION:
                        return new ReleaseBackupOperation();
                    case RELEASE_OPERATION:
                        return new ReleaseOperation();
                    case SEMAPHORE_DEAD_MEMBER_OPERATION:
                        return new SemaphoreDeadMemberOperation();
                    case SEMAPHORE_REPLICATION_OPERATION:
                        return new SemaphoreReplicationOperation();
                    default:
                        return null;
                }
            }
        };
    }
}
