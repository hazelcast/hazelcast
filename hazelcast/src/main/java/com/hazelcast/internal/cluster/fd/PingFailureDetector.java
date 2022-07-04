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

package com.hazelcast.internal.cluster.fd;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Ping based failure detector. (OSI Layer 3)
 * This failure detector uses an absolute number of missing ping attempts.
 * After that many attempts, an endpoint is considered as dead/unavailable.
 */
public class PingFailureDetector<E> {

    private final int maxPingAttempts;

    private final ConcurrentMap<E, AtomicInteger> pingAttempts = new ConcurrentHashMap<>();

    public PingFailureDetector(int maxPingAttempts) {
        this.maxPingAttempts = maxPingAttempts;
    }

    public int heartbeat(E endpoint) {
        return getAttempts(endpoint).getAndSet(0);
    }

    public void logAttempt(E endpoint) {
        getAttempts(endpoint).incrementAndGet();
    }

    public boolean isAlive(E endpoint) {
        AtomicInteger attempts = pingAttempts.get(endpoint);
        return attempts != null && attempts.get() < maxPingAttempts;
    }

    public void retainAttemptsForAliveEndpoints(Collection<E> aliveEndpoints) {
        pingAttempts.keySet().retainAll(aliveEndpoints);
    }

    public void remove(E endpoint) {
        pingAttempts.remove(endpoint);
    }

    public void reset() {
        pingAttempts.clear();
    }

    private AtomicInteger getAttempts(E endpoint) {
        AtomicInteger existing = pingAttempts.get(endpoint);
        AtomicInteger newAttempts = null;
        if (existing == null) {
            newAttempts = new AtomicInteger();
            existing = pingAttempts.putIfAbsent(endpoint, newAttempts);
        }

        return existing != null ? existing : newAttempts;
    }
}

