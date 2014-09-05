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
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public final class AtomicLongDataSerializerHook implements DataSerializerHook {

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
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case ADD_BACKUP:
                        return new AddBackupOperation();
                    case ADD_AND_GET:
                        return new AddAndGetOperation();
                    case ALTER:
                        return new AlterOperation();
                    case ALTER_AND_GET:
                        return new AlterAndGetOperation();
                    case APPLY:
                        return new ApplyOperation();
                    case COMPARE_AND_SET:
                        return new CompareAndSetOperation();
                    case GET:
                        return new GetOperation();
                    case GET_AND_SET:
                        return new GetAndSetOperation();
                    case GET_AND_ALTER:
                        return new GetAndAlterOperation();
                    case GET_AND_ADD:
                        return new GetAndAddOperation();
                    case SET_OPERATION:
                        return new SetOperation();
                    case SET_BACKUP:
                        return new SetBackupOperation();
                    case REPLICATION:
                        return new AtomicLongReplicationOperation();
                    default:
                        return null;
                }
            }
        };
    }
}
