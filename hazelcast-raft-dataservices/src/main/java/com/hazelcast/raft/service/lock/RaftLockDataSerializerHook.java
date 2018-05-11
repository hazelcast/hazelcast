/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.raft.service.lock.operation.GetLockCountOp;
import com.hazelcast.raft.service.lock.operation.GetLockFenceOp;
import com.hazelcast.raft.service.lock.operation.InvalidateWaitEntriesOp;
import com.hazelcast.raft.service.lock.operation.LockOp;
import com.hazelcast.raft.service.lock.operation.TryLockOp;
import com.hazelcast.raft.service.lock.operation.UnlockOp;

public class RaftLockDataSerializerHook implements DataSerializerHook {
    private static final int RAFT_LOCK_DS_FACTORY_ID = -3011;
    private static final String RAFT_LOCK_DS_FACTORY = "hazelcast.serialization.ds.raft.lock";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_LOCK_DS_FACTORY, RAFT_LOCK_DS_FACTORY_ID);

    public static final int LOCK_REGISTRY_SNAPSHOT = 1;
    public static final int RAFT_LOCK_SNAPSHOT = 2;
    public static final int LOCK_ENDPOINT = 3;
    public static final int LOCK_INVOCATION_KEY = 4;
    public static final int LOCK_OP = 5;
    public static final int TRY_LOCK_OP = 6;
    public static final int UNLOCK_OP = 7;
    public static final int FORCE_UNLOCK_OP = 8;
    public static final int GET_LOCK_COUNT_OP = 9;
    public static final int GET_LOCK_FENCE_OP = 10;
    public static final int INVALIDATE_WAI_ENTRIES_OP = 11;


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
                    case LOCK_REGISTRY_SNAPSHOT:
                        return new LockRegistrySnapshot();
                    case RAFT_LOCK_SNAPSHOT:
                        return new RaftLockSnapshot();
                    case LOCK_ENDPOINT:
                        return new LockEndpoint();
                    case LOCK_INVOCATION_KEY:
                        return new LockInvocationKey();
                    case LOCK_OP:
                        return new LockOp();
                    case TRY_LOCK_OP:
                        return new TryLockOp();
                    case UNLOCK_OP:
                        return new UnlockOp();
                    case FORCE_UNLOCK_OP:
                        return new ForceUnlockOp();
                    case GET_LOCK_COUNT_OP:
                        return new GetLockCountOp();
                    case GET_LOCK_FENCE_OP:
                        return new GetLockFenceOp();
                    case INVALIDATE_WAI_ENTRIES_OP:
                        return new InvalidateWaitEntriesOp();
                }
                throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
