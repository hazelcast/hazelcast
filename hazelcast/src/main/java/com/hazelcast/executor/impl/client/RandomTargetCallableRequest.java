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
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.Callable;

/**
 * A {@link com.hazelcast.client.impl.client.TargetClientRequest} which sends a {@link java.util.concurrent.Callable} task
 * to a random target. That random target address is decided by the given target creator function.
 */
public class RandomTargetCallableRequest extends AbstractTargetCallableRequest implements RefreshableRequest {

    private ConstructorFunction<Object, Address> targetCreator;

    public RandomTargetCallableRequest() {
    }

    public RandomTargetCallableRequest(String name, String uuid, Callable callable,
                                       ConstructorFunction<Object, Address> targetCreator) {
        super(name, uuid, callable, targetCreator.createNew(null));
        this.targetCreator = targetCreator;
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
        return ExecutorPortableHook.RANDOM_TARGET_CALLABLE_REQUEST;
    }

    @Override
    public void refresh() {
        final Address target = targetCreator.createNew(null);
        setTarget(target);
    }
}
