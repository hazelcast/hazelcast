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

package com.hazelcast.cp.internal.datastructures.lock;

import com.hazelcast.cp.internal.datastructures.lock.operation.GetLockOwnershipStateOp;
import com.hazelcast.cp.internal.datastructures.lock.operation.LockOp;
import com.hazelcast.cp.internal.datastructures.lock.operation.TryLockOp;
import com.hazelcast.cp.internal.datastructures.lock.operation.UnlockOp;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;

@SuppressWarnings("checkstyle:declarationorder")
public class LockDataSerializerHook implements DataSerializerHook {
    private static final int RAFT_LOCK_DS_FACTORY_ID = -1012;
    private static final String RAFT_LOCK_DS_FACTORY = "hazelcast.serialization.ds.raft.lock";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_LOCK_DS_FACTORY, RAFT_LOCK_DS_FACTORY_ID);

    public static final int RAFT_LOCK_REGISTRY = 1;
    public static final int RAFT_LOCK = 2;
    public static final int LOCK_ENDPOINT = 3;
    public static final int LOCK_INVOCATION_KEY = 4;
    public static final int RAFT_LOCK_OWNERSHIP_STATE = 5;
    public static final int LOCK_OP = 6;
    public static final int TRY_LOCK_OP = 7;
    public static final int UNLOCK_OP = 8;
    public static final int GET_RAFT_LOCK_OWNERSHIP_STATE_OP = 9;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case RAFT_LOCK_REGISTRY:
                    return new LockRegistry();
                case RAFT_LOCK:
                    return new Lock();
                case LOCK_ENDPOINT:
                    return new LockEndpoint();
                case LOCK_INVOCATION_KEY:
                    return new LockInvocationKey();
                case RAFT_LOCK_OWNERSHIP_STATE:
                    return new LockOwnershipState();
                case LOCK_OP:
                    return new LockOp();
                case TRY_LOCK_OP:
                    return new TryLockOp();
                case UNLOCK_OP:
                    return new UnlockOp();
                case GET_RAFT_LOCK_OWNERSHIP_STATE_OP:
                    return new GetLockOwnershipStateOp();
                default:
                    throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
