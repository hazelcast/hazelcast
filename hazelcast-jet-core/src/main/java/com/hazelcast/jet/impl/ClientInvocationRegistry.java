/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.spi.impl.SimpleExecutionCallback;

import java.util.concurrent.ConcurrentHashMap;

public class ClientInvocationRegistry {
    private final ConcurrentHashMap<Long, ICompletableFuture<Object>> clientInvocations = new ConcurrentHashMap<>();

    public void register(long executionId, ICompletableFuture<Object> invocation) {
        if (clientInvocations.putIfAbsent(executionId, invocation) != null) {
            throw new IllegalStateException("Execution with id " + executionId + " is already registered.");
        }
        invocation.andThen(new SimpleExecutionCallback<Object>() {
            @Override
            public void notify(Object o) {
                clientInvocations.remove(executionId);
            }
        });
    }

    public void cancel(long executionId) {
        ICompletableFuture<Object> f = clientInvocations.get(executionId);
        if (f != null) {
            f.cancel(true);
        }
    }

}
