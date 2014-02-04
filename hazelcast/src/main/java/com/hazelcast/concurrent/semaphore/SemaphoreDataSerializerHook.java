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

import com.hazelcast.nio.serialization.*;
import com.hazelcast.util.ConstructorFunction;

public class SemaphoreDataSerializerHook implements DataSerializerHook {

    static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.SEMAPHORE_DS_FACTORY, -16);

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
