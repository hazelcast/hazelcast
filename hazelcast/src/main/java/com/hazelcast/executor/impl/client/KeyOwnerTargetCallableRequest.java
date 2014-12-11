/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.util.concurrent.Callable;

/**
 * A {@link com.hazelcast.client.impl.client.TargetClientRequest} which includes a {@link java.util.concurrent.Callable}
 * and finds a target address to sent that callable according to given partitionId of a key.
 */
public class KeyOwnerTargetCallableRequest extends AbstractTargetCallableRequest {

    public KeyOwnerTargetCallableRequest() {
    }

    public KeyOwnerTargetCallableRequest(String name, String uuid, Callable callable, int partitionId) {
        super(name, uuid, callable, partitionId);
    }

    @Override
    public Operation getOperation(String name, String uuid, Data callableData) {
        return new CallableTaskOperation(name, uuid, callableData);
    }

    @Override
    public Address getTarget() {
        return null;
    }

    /**
     * Returns class identifier for this portable class. Class id should be unique per PortableFactory.
     *
     * @return class id
     */
    @Override
    public int getClassId() {
        return ExecutorPortableHook.KEY_OWNER_TARGET_CALLABLE_REQUEST;
    }
}
