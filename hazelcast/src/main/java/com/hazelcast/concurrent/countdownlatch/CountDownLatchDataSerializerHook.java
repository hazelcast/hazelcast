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

package com.hazelcast.concurrent.countdownlatch;

import com.hazelcast.concurrent.countdownlatch.operations.AwaitOperation;
import com.hazelcast.concurrent.countdownlatch.operations.CountDownLatchBackupOperation;
import com.hazelcast.concurrent.countdownlatch.operations.CountDownLatchReplicationOperation;
import com.hazelcast.concurrent.countdownlatch.operations.CountDownOperation;
import com.hazelcast.concurrent.countdownlatch.operations.GetCountOperation;
import com.hazelcast.concurrent.countdownlatch.operations.SetCountOperation;
import com.hazelcast.nio.serialization.AbstractDataSerializerHook;
import com.hazelcast.nio.serialization.ArrayDataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;

public final class CountDownLatchDataSerializerHook extends AbstractDataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.CDL_PORTABLE_FACTORY, -14);

    public static final int AWAIT_OPERATION = 0;
    public static final int COUNT_DOWN_LATCH_BACKUP_OPERATION = 1;
    public static final int COUNT_DOWN_LATCH_REPLICATION_OPERATION = 2;
    public static final int COUNT_DOWN_OPERATION = 3;
    public static final int GET_COUNT_OPERATION = 4;
    public static final int SET_COUNT_OPERATION = 5;

    private static final int LEN = SET_COUNT_OPERATION + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];
        constructors[AWAIT_OPERATION] = createFunction(new AwaitOperation());
        constructors[COUNT_DOWN_LATCH_BACKUP_OPERATION] = createFunction(new CountDownLatchBackupOperation());
        constructors[COUNT_DOWN_LATCH_REPLICATION_OPERATION] = createFunction(new CountDownLatchReplicationOperation());
        constructors[COUNT_DOWN_OPERATION] = createFunction(new CountDownOperation());
        constructors[GET_COUNT_OPERATION] = createFunction(new GetCountOperation());
        constructors[SET_COUNT_OPERATION] = createFunction(new SetCountOperation());
        return new ArrayDataSerializableFactory(constructors);
    }
}
