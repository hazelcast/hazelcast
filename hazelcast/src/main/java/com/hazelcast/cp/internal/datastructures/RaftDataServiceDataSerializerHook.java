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

package com.hazelcast.cp.internal.datastructures;

import com.hazelcast.cp.internal.datastructures.spi.blocking.WaitKeyContainer;
import com.hazelcast.cp.internal.datastructures.spi.blocking.operation.ExpireWaitKeysOp;
import com.hazelcast.cp.internal.datastructures.spi.operation.DestroyRaftObjectOp;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;

/**
 */
@SuppressWarnings("checkstyle:declarationorder")
public class RaftDataServiceDataSerializerHook implements DataSerializerHook {

    private static final int FACTORY_ID = -1010;
    private static final String RAFT_DS_FACTORY = "hazelcast.serialization.ds.raft.data";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_DS_FACTORY, FACTORY_ID);


    public static final int WAIT_KEY_CONTAINER = 1;
    public static final int EXPIRE_WAIT_KEYS_OP = 2;
    public static final int DESTROY_RAFT_OBJECT_OP = 3;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case WAIT_KEY_CONTAINER:
                    return new WaitKeyContainer();
                case EXPIRE_WAIT_KEYS_OP:
                    return new ExpireWaitKeysOp();
                case DESTROY_RAFT_OBJECT_OP:
                    return new DestroyRaftObjectOp();
                default:
                    throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
