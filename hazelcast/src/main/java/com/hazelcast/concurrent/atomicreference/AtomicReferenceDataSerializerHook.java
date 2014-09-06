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

package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.concurrent.atomicreference.operations.AlterAndGetOperation;
import com.hazelcast.concurrent.atomicreference.operations.AlterOperation;
import com.hazelcast.concurrent.atomicreference.operations.ApplyOperation;
import com.hazelcast.concurrent.atomicreference.operations.AtomicReferenceReplicationOperation;
import com.hazelcast.concurrent.atomicreference.operations.CompareAndSetOperation;
import com.hazelcast.concurrent.atomicreference.operations.ContainsOperation;
import com.hazelcast.concurrent.atomicreference.operations.GetAndAlterOperation;
import com.hazelcast.concurrent.atomicreference.operations.GetAndSetOperation;
import com.hazelcast.concurrent.atomicreference.operations.GetOperation;
import com.hazelcast.concurrent.atomicreference.operations.IsNullOperation;
import com.hazelcast.concurrent.atomicreference.operations.SetAndGetOperation;
import com.hazelcast.concurrent.atomicreference.operations.SetBackupOperation;
import com.hazelcast.concurrent.atomicreference.operations.SetOperation;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public final class AtomicReferenceDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.ATOMIC_REFERENCE_DS_FACTORY, -21);

    public static final int ALTER_AND_GET = 0;
    public static final int ALTER = 1;
    public static final int APPLY = 2;
    public static final int COMPARE_AND_SET = 3;
    public static final int CONTAINS = 4;
    public static final int GET_AND_ALTER = 5;
    public static final int GET_AND_SET = 6;
    public static final int GET = 7;
    public static final int IS_NULL = 8;
    public static final int SET_AND_GET = 9;
    public static final int SET_BACKUP = 10;
    public static final int SET = 11;
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
                    case ALTER_AND_GET:
                        return new AlterAndGetOperation();
                    case ALTER:
                        return new AlterOperation();
                    case APPLY:
                        return new ApplyOperation();
                    case COMPARE_AND_SET:
                        return new CompareAndSetOperation();
                    case CONTAINS:
                        return new ContainsOperation();
                    case GET_AND_ALTER:
                        return new GetAndAlterOperation();
                    case GET_AND_SET:
                        return new GetAndSetOperation();
                    case GET:
                        return new GetOperation();
                    case IS_NULL:
                        return new IsNullOperation();
                    case SET_AND_GET:
                        return new SetAndGetOperation();
                    case SET_BACKUP:
                        return new SetBackupOperation();
                    case SET:
                        return new SetOperation();
                    case REPLICATION:
                        return new AtomicReferenceReplicationOperation();
                    default:
                        return null;
                }
            }
        };
    }
}
