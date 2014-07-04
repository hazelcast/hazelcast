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

package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.concurrent.atomiclong.operations.AddAndGetOperation;
import com.hazelcast.concurrent.atomiclong.operations.AddBackupOperation;
import com.hazelcast.concurrent.atomiclong.operations.AlterAndGetOperation;
import com.hazelcast.concurrent.atomiclong.operations.AlterOperation;
import com.hazelcast.concurrent.atomiclong.operations.ApplyOperation;
import com.hazelcast.concurrent.atomiclong.operations.AtomicLongReplicationOperation;
import com.hazelcast.concurrent.atomiclong.operations.CompareAndSetOperation;
import com.hazelcast.concurrent.atomiclong.operations.GetAndAddOperation;
import com.hazelcast.concurrent.atomiclong.operations.GetAndAlterOperation;
import com.hazelcast.concurrent.atomiclong.operations.GetAndSetOperation;
import com.hazelcast.concurrent.atomiclong.operations.GetOperation;
import com.hazelcast.concurrent.atomiclong.operations.SetBackupOperation;
import com.hazelcast.concurrent.atomiclong.operations.SetOperation;
import com.hazelcast.nio.serialization.AbstractDataSerializerHook;
import com.hazelcast.nio.serialization.ArrayDataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;

public final class AtomicLongDataSerializerHook extends AbstractDataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.ATOMIC_LONG_DS_FACTORY, -17);

    public static final int ADD_BACKUP = 0;
    public static final int ADD_AND_GET = 1;
    public static final int ALTER = 2;
    public static final int ALTER_AND_GET = 3;
    public static final int APPLY = 4;
    public static final int COMPARE_AND_SET = 5;
    public static final int GET = 6;
    public static final int GET_AND_SET = 7;
    public static final int GET_AND_ALTER = 8;
    public static final int GET_AND_ADD = 9;
    public static final int SET_OPERATION = 10;
    public static final int SET_BACKUP = 11;
    public static final int REPLICATION = 12;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[REPLICATION + 1];
        constructors[ADD_BACKUP] = createFunction(new AddBackupOperation());
        constructors[ADD_AND_GET] = createFunction(new AddAndGetOperation());
        constructors[ALTER] = createFunction(new AlterOperation());
        constructors[ALTER_AND_GET] = createFunction(new AlterAndGetOperation());
        constructors[APPLY] = createFunction(new ApplyOperation());
        constructors[COMPARE_AND_SET] = createFunction(new CompareAndSetOperation());
        constructors[GET] = createFunction(new GetOperation());
        constructors[GET_AND_SET] = createFunction(new GetAndSetOperation());
        constructors[GET_AND_ALTER] = createFunction(new GetAndAlterOperation());
        constructors[GET_AND_ADD] = createFunction(new GetAndAddOperation());
        constructors[SET_OPERATION] = createFunction(new SetOperation());
        constructors[SET_BACKUP] = createFunction(new SetBackupOperation());
        constructors[REPLICATION] = createFunction(new AtomicLongReplicationOperation());
        return new ArrayDataSerializableFactory(constructors);
    }
}
