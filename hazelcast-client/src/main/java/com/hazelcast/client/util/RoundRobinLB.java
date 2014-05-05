/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.util;

import com.hazelcast.core.Member;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link com.hazelcast.client.LoadBalancer} implementation that relies on using round robin
 * to a next member to send a request to.
 * <p/>
 * Round robin is done based on best effort basis, the order of members for concurrent calls to
 * the {@link #next()} is not guaranteed.
 */
public class RoundRobinLB extends AbstractLoadBalancer {

    private final AtomicInteger indexRef;

    public RoundRobinLB() {
        this((int) System.nanoTime());
    }

    public RoundRobinLB(int seed) {
        indexRef = new AtomicInteger(seed);
    }

    @Override
    public Member next() {
        Member[] members = getMembers();
        if (members == null || members.length == 0) {
            return null;
        }
        int length = members.length;
        int index = (indexRef.getAndAdd(1) % length + length) % length;
        return members[index];
    }
}
