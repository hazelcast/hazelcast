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
import com.hazelcast.nio.serialization.AbstractDataSerializerHook;
import com.hazelcast.nio.serialization.ArrayDataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;

public class SemaphoreDataSerializerHook extends AbstractDataSerializerHook {

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

    private static final int LEN = SEMAPHORE_REPLICATION_OPERATION + 1;


    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];
        constructors[ACQUIRE_BACKUP_OPERATION] = createFunction(new AcquireBackupOperation());
        constructors[ACQUIRE_OPERATION] = createFunction(new AcquireOperation());
        constructors[AVAILABLE_OPERATION] = createFunction(new AvailableOperation());
        constructors[DRAIN_BACKUP_OPERATION] = createFunction(new DrainBackupOperation());
        constructors[DRAIN_OPERATION] = createFunction(new DrainOperation());
        constructors[INIT_BACKUP_OPERATION] = createFunction(new InitBackupOperation());
        constructors[INIT_OPERATION] = createFunction(new InitOperation());
        constructors[RELEASE_BACKUP_OPERATION] = createFunction(new ReduceBackupOperation());
        constructors[REDUCE_OPERATION] = createFunction(new ReduceOperation());
        constructors[RELEASE_BACKUP_OPERATION] = createFunction(new ReleaseBackupOperation());
        constructors[RELEASE_OPERATION] = createFunction(new ReleaseOperation());
        constructors[DEAD_MEMBER_BACKUP_OPERATION] = createFunction(new DeadMemberBackupOperation());
        constructors[SEMAPHORE_DEAD_MEMBER_OPERATION] = createFunction(new SemaphoreDeadMemberOperation());
        constructors[SEMAPHORE_REPLICATION_OPERATION] = createFunction(new SemaphoreReplicationOperation());
        return new ArrayDataSerializableFactory(constructors);
    }

}
