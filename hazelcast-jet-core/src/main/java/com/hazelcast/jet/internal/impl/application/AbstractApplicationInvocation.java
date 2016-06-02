/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.internal.impl.application;

import com.hazelcast.nio.Address;

import java.util.concurrent.Callable;

public abstract class AbstractApplicationInvocation<Instance, T> implements Callable<T> {
    private final Address address;
    private final Instance operation;

    public AbstractApplicationInvocation(Instance operation,
                                         Address address) {
        this.address = address;
        this.operation = operation;
    }

    @Override
    public T call() {
        return execute(this.operation, this.address);
    }

    @SuppressWarnings("unchecked")
    protected abstract T execute(Instance operation, Address address);
}
