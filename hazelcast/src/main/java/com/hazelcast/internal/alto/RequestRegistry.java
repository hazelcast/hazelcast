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

package com.hazelcast.internal.alto;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class RequestRegistry {

    private final int concurrentRequestLimit;
    private final long nextCallIdTimeoutMs = TimeUnit.SECONDS.toMillis(10);
    private final ConcurrentMap<SocketAddress, Requests> requestsPerSocket = new ConcurrentHashMap<>();
    private final Requests[] requestsPerPartitions;

    public RequestRegistry(int concurrentRequestLimit, int partitions) {
        this.concurrentRequestLimit = concurrentRequestLimit;
        this.requestsPerPartitions = new Requests[partitions];
        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            requestsPerPartitions[partitionId] = new Requests(concurrentRequestLimit, nextCallIdTimeoutMs);
        }
    }

    public Requests getByPartitionId(int partitionId) {
        return requestsPerPartitions[partitionId];
    }

    Requests getByAddress(SocketAddress address) {
        return requestsPerSocket.get(address);
    }

    public Requests getRequestsOrCreate(SocketAddress address) {
        Requests requests = requestsPerSocket.get(address);
        if (requests == null) {
            Requests newRequests = new Requests(concurrentRequestLimit, nextCallIdTimeoutMs);
            Requests foundRequests = requestsPerSocket.putIfAbsent(address, newRequests);
            return foundRequests == null ? newRequests : foundRequests;
        } else {
            return requests;
        }
    }

    void shutdown() {
        for (Requests requests : requestsPerSocket.values()) {
            requests.shutdown();
        }
    }
}
