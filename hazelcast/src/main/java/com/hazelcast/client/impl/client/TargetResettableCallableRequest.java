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

package com.hazelcast.client.impl.client;

import com.hazelcast.executor.impl.ExecutorPortableHook;
import com.hazelcast.executor.impl.client.TargetCallableRequest;
import com.hazelcast.executor.impl.operations.CallableTaskOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.util.concurrent.Callable;

/**
 * A request which resets its target in case of a resend operation.
 */
public class TargetResettableCallableRequest extends TargetCallableRequest implements TargetResettable {

    private TargetResettable targetResettable;

    public TargetResettableCallableRequest() {
    }

    public TargetResettableCallableRequest(String name, String uuid,
                                           Callable callable,
                                           TargetResettable targetResettable) {
        super(name, uuid, callable, targetResettable.reset());
        this.targetResettable = targetResettable;
    }

    @Override
    protected Operation getOperation(String name, String uuid, Data callableData) {
        return new CallableTaskOperation(name, uuid, callableData);
    }

    @Override
    public Address reset() {
        final Address address = targetResettable.reset();
        setTarget(address);
        return address;
    }

    @Override
    public int getClassId() {
        return ExecutorPortableHook.TARGET_RESETTABLE_REQUEST;
    }
}
