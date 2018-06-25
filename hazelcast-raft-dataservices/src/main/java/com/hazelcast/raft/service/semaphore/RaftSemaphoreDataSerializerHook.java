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

package com.hazelcast.raft.service.semaphore;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.service.semaphore.operation.AcquirePermitsOp;
import com.hazelcast.raft.service.semaphore.operation.AvailablePermitsOp;
import com.hazelcast.raft.service.semaphore.operation.ChangePermitsOp;
import com.hazelcast.raft.service.semaphore.operation.DrainPermitsOp;
import com.hazelcast.raft.service.semaphore.operation.InitSemaphoreOp;
import com.hazelcast.raft.service.semaphore.operation.ReleasePermitsOp;

@SuppressWarnings("checkstyle:declarationorder")
public class RaftSemaphoreDataSerializerHook implements DataSerializerHook {
    private static final int DS_FACTORY_ID = -1013;
    private static final String DS_FACTORY = "hazelcast.serialization.ds.raft.sema";

    public static final int F_ID = FactoryIdHelper.getFactoryId(DS_FACTORY, DS_FACTORY_ID);

    public static final int SEMAPHORE_REGISTRY = 1;
    public static final int RAFT_SEMAPHORE = 2;
    public static final int SEMAPHORE_INVOCATION_KEY = 3;
    public static final int ACQUIRE_PERMITS_OP = 4;
    public static final int AVAILABLE_PERMITS_OP = 5;
    public static final int CHANGE_PERMITS_OP = 6;
    public static final int DRAIN_PERMITS_OP = 7;
    public static final int INIT_SEMAPHORE_OP = 8;
    public static final int RELEASE_PERMITS_OP = 9;

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
                    case SEMAPHORE_REGISTRY:
                        return new SemaphoreRegistry();
                    case RAFT_SEMAPHORE:
                        return new RaftSemaphore();
                    case SEMAPHORE_INVOCATION_KEY:
                        return new SemaphoreInvocationKey();
                    case ACQUIRE_PERMITS_OP:
                        return new AcquirePermitsOp();
                    case AVAILABLE_PERMITS_OP:
                        return new AvailablePermitsOp();
                    case CHANGE_PERMITS_OP:
                        return new ChangePermitsOp();
                    case DRAIN_PERMITS_OP:
                        return new DrainPermitsOp();
                    case INIT_SEMAPHORE_OP:
                        return new InitSemaphoreOp();
                    case RELEASE_PERMITS_OP:
                        return new ReleasePermitsOp();
                    default:
                        throw new IllegalArgumentException("Undefined type: " + typeId);
                }
            }
        };
    }
}
