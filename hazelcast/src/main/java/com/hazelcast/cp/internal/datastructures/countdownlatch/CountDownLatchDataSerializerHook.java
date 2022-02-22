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

package com.hazelcast.cp.internal.datastructures.countdownlatch;

import com.hazelcast.cp.internal.datastructures.countdownlatch.operation.AwaitOp;
import com.hazelcast.cp.internal.datastructures.countdownlatch.operation.CountDownOp;
import com.hazelcast.cp.internal.datastructures.countdownlatch.operation.GetCountOp;
import com.hazelcast.cp.internal.datastructures.countdownlatch.operation.GetRoundOp;
import com.hazelcast.cp.internal.datastructures.countdownlatch.operation.TrySetCountOp;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;

@SuppressWarnings("checkstyle:declarationorder")
public class CountDownLatchDataSerializerHook implements DataSerializerHook {
    private static final int RAFT_COUNT_DOWN_LATCH_DS_FACTORY_ID = -1015;
    private static final String RAFT_COUNT_DOWN_LATCH_DS_FACTORY = "hazelcast.serialization.ds.raft.countdownlatch";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_COUNT_DOWN_LATCH_DS_FACTORY,
            RAFT_COUNT_DOWN_LATCH_DS_FACTORY_ID);

    public static final int COUNT_DOWN_LATCH_REGISTRY = 1;
    public static final int COUNT_DOWN_LATCH = 2;
    public static final int AWAIT_INVOCATION_KEY = 3;
    public static final int AWAIT_OP = 4;
    public static final int COUNT_DOWN_OP = 5;
    public static final int GET_COUNT_OP = 6;
    public static final int GET_ROUND_OP = 7;
    public static final int TRY_SET_COUNT_OP = 8;


    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case COUNT_DOWN_LATCH_REGISTRY:
                    return new CountDownLatchRegistry();
                case COUNT_DOWN_LATCH:
                    return new CountDownLatch();
                case AWAIT_INVOCATION_KEY:
                    return new AwaitInvocationKey();
                case AWAIT_OP:
                    return new AwaitOp();
                case COUNT_DOWN_OP:
                    return new CountDownOp();
                case GET_COUNT_OP:
                    return new GetCountOp();
                case GET_ROUND_OP:
                    return new GetRoundOp();
                case TRY_SET_COUNT_OP:
                    return new TrySetCountOp();
                default:
                    throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
