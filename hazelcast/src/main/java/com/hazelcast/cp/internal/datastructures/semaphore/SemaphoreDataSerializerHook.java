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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.cp.internal.datastructures.semaphore.operation.AcquirePermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.AvailablePermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.ChangePermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.DrainPermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.InitSemaphoreOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.ReleasePermitsOp;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;

@SuppressWarnings("checkstyle:declarationorder")
public class SemaphoreDataSerializerHook implements DataSerializerHook {
    private static final int DS_FACTORY_ID = -1013;
    private static final String DS_FACTORY = "hazelcast.serialization.ds.raft.sema";

    public static final int F_ID = FactoryIdHelper.getFactoryId(DS_FACTORY, DS_FACTORY_ID);

    public static final int RAFT_SEMAPHORE_REGISTRY = 1;
    public static final int RAFT_SEMAPHORE = 2;
    public static final int ACQUIRE_INVOCATION_KEY = 3;
    public static final int SEMAPHORE_ENDPOINT = 4;
    public static final int ACQUIRE_PERMITS_OP = 5;
    public static final int AVAILABLE_PERMITS_OP = 6;
    public static final int CHANGE_PERMITS_OP = 7;
    public static final int DRAIN_PERMITS_OP = 8;
    public static final int INIT_SEMAPHORE_OP = 9;
    public static final int RELEASE_PERMITS_OP = 10;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case RAFT_SEMAPHORE_REGISTRY:
                    return new SemaphoreRegistry();
                case RAFT_SEMAPHORE:
                    return new Semaphore();
                case ACQUIRE_INVOCATION_KEY:
                    return new AcquireInvocationKey();
                case SEMAPHORE_ENDPOINT:
                    return new SemaphoreEndpoint();
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
        };
    }
}
