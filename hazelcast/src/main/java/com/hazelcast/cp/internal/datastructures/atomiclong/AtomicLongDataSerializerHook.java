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

package com.hazelcast.cp.internal.datastructures.atomiclong;

import com.hazelcast.cp.internal.datastructures.atomiclong.operation.AddAndGetOp;
import com.hazelcast.cp.internal.datastructures.atomiclong.operation.AlterOp;
import com.hazelcast.cp.internal.datastructures.atomiclong.operation.ApplyOp;
import com.hazelcast.cp.internal.datastructures.atomiclong.operation.CompareAndSetOp;
import com.hazelcast.cp.internal.datastructures.atomiclong.operation.GetAndAddOp;
import com.hazelcast.cp.internal.datastructures.atomiclong.operation.GetAndSetOp;
import com.hazelcast.cp.internal.datastructures.atomiclong.operation.LocalGetOp;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;

@SuppressWarnings("checkstyle:declarationorder")
public final class AtomicLongDataSerializerHook implements DataSerializerHook {

    private static final int RAFT_ATOMIC_LONG_DS_FACTORY_ID = -1011;
    private static final String RAFT_ATOMIC_LONG_DS_FACTORY = "hazelcast.serialization.ds.raft.atomiclong";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_ATOMIC_LONG_DS_FACTORY, RAFT_ATOMIC_LONG_DS_FACTORY_ID);

    public static final int ADD_AND_GET_OP = 1;
    public static final int COMPARE_AND_SET_OP = 2;
    public static final int GET_AND_ADD_OP = 3;
    public static final int GET_AND_SET_OP = 4;
    public static final int ALTER_OP = 5;
    public static final int APPLY_OP = 6;
    public static final int LOCAL_GET_OP = 7;
    public static final int SNAPSHOT = 8;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case ADD_AND_GET_OP:
                    return new AddAndGetOp();
                case COMPARE_AND_SET_OP:
                    return new CompareAndSetOp();
                case GET_AND_ADD_OP:
                    return new GetAndAddOp();
                case GET_AND_SET_OP:
                    return new GetAndSetOp();
                case ALTER_OP:
                    return new AlterOp();
                case APPLY_OP:
                    return new ApplyOp();
                case LOCAL_GET_OP:
                    return new LocalGetOp();
                case SNAPSHOT:
                    return new AtomicLongSnapshot();
                default:
                    throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
