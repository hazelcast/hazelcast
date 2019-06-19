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

package com.hazelcast.cp.internal.datastructures.unsafe.semaphore;

import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.operations.AcquireBackupOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.operations.AcquireOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.operations.AvailableOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.operations.DrainBackupOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.operations.DrainOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.operations.IncreaseBackupOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.operations.IncreaseOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.operations.InitBackupOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.operations.InitOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.operations.ReduceBackupOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.operations.ReduceOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.operations.ReleaseBackupOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.operations.ReleaseOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.operations.SemaphoreDetachMemberBackupOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.operations.SemaphoreDetachMemberOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.operations.SemaphoreReplicationOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SEMAPHORE_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SEMAPHORE_DS_FACTORY_ID;

public class SemaphoreDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(SEMAPHORE_DS_FACTORY, SEMAPHORE_DS_FACTORY_ID);

    public static final int CONTAINER = 0;
    public static final int ACQUIRE_BACKUP_OPERATION = 1;
    public static final int ACQUIRE_OPERATION = 2;
    public static final int AVAILABLE_OPERATION = 3;
    public static final int DETACH_MEMBER_BACKUP_OPERATION = 4;
    public static final int DRAIN_BACKUP_OPERATION = 5;
    public static final int DRAIN_OPERATION = 6;
    public static final int INIT_BACKUP_OPERATION = 7;
    public static final int INIT_OPERATION = 8;
    public static final int REDUCE_BACKUP_OPERATION = 9;
    public static final int REDUCE_OPERATION = 10;
    public static final int RELEASE_BACKUP_OPERATION = 11;
    public static final int RELEASE_OPERATION = 12;
    public static final int DETACH_MEMBER_OPERATION = 13;
    public static final int SEMAPHORE_REPLICATION_OPERATION = 14;
    public static final int INCREASE_OPERATION = 15;
    public static final int INCREASE_BACKUP_OPERATION = 16;

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
                    case CONTAINER:
                        return new SemaphoreContainer();
                    case ACQUIRE_BACKUP_OPERATION:
                        return new AcquireBackupOperation();
                    case ACQUIRE_OPERATION:
                        return new AcquireOperation();
                    case AVAILABLE_OPERATION:
                        return new AvailableOperation();
                    case DETACH_MEMBER_BACKUP_OPERATION:
                        return new SemaphoreDetachMemberBackupOperation();
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
                    case DETACH_MEMBER_OPERATION:
                        return new SemaphoreDetachMemberOperation();
                    case SEMAPHORE_REPLICATION_OPERATION:
                        return new SemaphoreReplicationOperation();
                    case INCREASE_OPERATION:
                        return new IncreaseOperation();
                    case INCREASE_BACKUP_OPERATION:
                        return new IncreaseBackupOperation();
                    default:
                        return null;
                }
            }
        };
    }
}
