/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.lock;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.service.lock.operation.ForceUnlockOp;
import com.hazelcast.raft.service.lock.operation.GetLockOwnershipStateOp;
import com.hazelcast.raft.service.lock.operation.LockOp;
import com.hazelcast.raft.service.lock.operation.TryLockOp;
import com.hazelcast.raft.service.lock.operation.UnlockOp;

@SuppressWarnings("checkstyle:declarationorder")
public class RaftLockDataSerializerHook implements DataSerializerHook {
    private static final int RAFT_LOCK_DS_FACTORY_ID = -1012;
    private static final String RAFT_LOCK_DS_FACTORY = "hazelcast.serialization.ds.raft.lock";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_LOCK_DS_FACTORY, RAFT_LOCK_DS_FACTORY_ID);

    public static final int LOCK_REGISTRY = 1;
    public static final int RAFT_LOCK = 2;
    public static final int LOCK_ENDPOINT = 3;
    public static final int LOCK_INVOCATION_KEY = 4;
    public static final int RAFT_LOCK_OWNERSHIP = 5;
    public static final int LOCK_OP = 6;
    public static final int TRY_LOCK_OP = 7;
    public static final int UNLOCK_OP = 8;
    public static final int FORCE_UNLOCK_OP = 9;
    public static final int GET_RAFT_LOCK_OWNERSHIP_STATE_OP = 10;

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
                    case LOCK_REGISTRY:
                        return new LockRegistry();
                    case RAFT_LOCK:
                        return new RaftLock();
                    case LOCK_ENDPOINT:
                        return new LockEndpoint();
                    case LOCK_INVOCATION_KEY:
                        return new LockInvocationKey();
                    case RAFT_LOCK_OWNERSHIP:
                        return new RaftLockOwnershipState();
                    case LOCK_OP:
                        return new LockOp();
                    case TRY_LOCK_OP:
                        return new TryLockOp();
                    case UNLOCK_OP:
                        return new UnlockOp();
                    case FORCE_UNLOCK_OP:
                        return new ForceUnlockOp();
                    case GET_RAFT_LOCK_OWNERSHIP_STATE_OP:
                        return new GetLockOwnershipStateOp();
                    default:
                        throw new IllegalArgumentException("Undefined type: " + typeId);
                }
            }
        };
    }
}
