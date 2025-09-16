/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.operationservice.LiveOperations;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LiveOperationRegistry {
    // memberAddress -> callId -> operation
    private final ConcurrentHashMap<Address, Map<Long, Operation>> liveOperations = new ConcurrentHashMap<>();

    public boolean register(Operation operation) {
        Map<Long, Operation> callIds = liveOperations.computeIfAbsent(operation.getCallerAddress(),
                key -> new ConcurrentHashMap<>());
        return callIds.putIfAbsent(operation.getCallId(), operation) == null;
    }

    public void deregister(Operation operation) {
        Map<Long, Operation> operations = liveOperations.get(operation.getCallerAddress());

        if (operations == null) {
            throw new IllegalStateException("Missing address during de-registration of operation=" + operation);
        }

        if (operations.remove(operation.getCallId()) == null) {
            throw new IllegalStateException("Missing operation during de-registration of operation=" + operation);
        }
    }

    public void populate(LiveOperations liveOperations) {
        this.liveOperations.forEach((key, value) -> value.keySet().forEach(callId -> liveOperations.add(key, callId)));
    }

}
