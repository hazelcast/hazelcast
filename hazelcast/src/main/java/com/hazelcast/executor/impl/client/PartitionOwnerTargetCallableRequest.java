/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.executor.impl.client;

import com.hazelcast.executor.impl.ExecutorPortableHook;
import com.hazelcast.executor.impl.operations.CallableTaskOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.util.concurrent.Callable;

/**
 * A {@link com.hazelcast.client.impl.client.TargetClientRequest} which sends a {@link java.util.concurrent.Callable} task
 * to a specific target. That specific target address is decided by using partitionId.
 */
public class PartitionOwnerTargetCallableRequest extends AbstractTargetCallableRequest {

    public PartitionOwnerTargetCallableRequest() {
    }

    public PartitionOwnerTargetCallableRequest(String name, String uuid, Callable callable, int partitionId) {
        super(name, uuid, callable, partitionId);
    }

    @Override
    public Operation getOperation(String name, String uuid, Data callableData) {
        return new CallableTaskOperation(name, uuid, callableData);
    }

    /**
     * Returns class identifier for this portable class. Class id should be unique per PortableFactory.
     *
     * @return class id
     */
    @Override
    public int getClassId() {
        return ExecutorPortableHook.PARTITION_OWNER_TARGET_CALLABLE_REQUEST;
    }
}
