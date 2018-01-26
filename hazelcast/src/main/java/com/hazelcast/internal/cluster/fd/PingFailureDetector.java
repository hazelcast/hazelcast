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

package com.hazelcast.internal.cluster.fd;

import com.hazelcast.core.Member;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Ping based failure detector. (OSI Layer 3)
 * This failure detector uses an absolute number of missing ping attempts.
 * After that many attempts, a member is considered as dead/unavailable.
 */
public class PingFailureDetector {

    private final int maxPingAttempts;

    private final ConcurrentMap<Member, AtomicInteger> pingAttempts = new ConcurrentHashMap<Member, AtomicInteger>();

    public PingFailureDetector(int maxPingAttempts) {
        this.maxPingAttempts = maxPingAttempts;
    }

    public void heartbeat(Member member) {
        getAttempts(member).set(0);
    }

    public void logAttempt(Member member) {
        getAttempts(member).incrementAndGet();
    }

    public boolean isAlive(Member member) {
        AtomicInteger attempts = pingAttempts.get(member);
        return attempts != null && attempts.get() < maxPingAttempts;
    }

    public void remove(Member member) {
        pingAttempts.remove(member);
    }

    public void reset() {
        pingAttempts.clear();
    }

    private AtomicInteger getAttempts(Member member) {
        AtomicInteger existing = pingAttempts.get(member);
        AtomicInteger newAttempts = null;
        if (existing == null) {
            newAttempts = new AtomicInteger();
            existing = pingAttempts.putIfAbsent(member, newAttempts);
        }

        return existing != null ? existing : newAttempts;
    }
}
