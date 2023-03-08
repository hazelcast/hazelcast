/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpc.server;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A registry with all pending requests.
 */
public class RequestsRegistry {

    private final int concurrentRequestLimit;
    private final long nextCallIdTimeoutMs = SECONDS.toMillis(10);
    private final ConcurrentMap<SocketAddress, Requests> requestsPerSocket = new ConcurrentHashMap<>();
    private final Requests[] partitionRequests;

    public RequestsRegistry(int concurrentRequestLimit, int partitions) {
        this.concurrentRequestLimit = concurrentRequestLimit;
        this.partitionRequests = new Requests[partitions];
        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            partitionRequests[partitionId] = new Requests(concurrentRequestLimit, nextCallIdTimeoutMs);
        }
    }

    public Requests getByPartitionId(int partitionId) {
        return partitionRequests[partitionId];
    }

    public Requests getByAddress(SocketAddress address) {
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

    public void shutdown() {
        for (Requests requests : requestsPerSocket.values()) {
            requests.shutdown();
        }
    }
}
