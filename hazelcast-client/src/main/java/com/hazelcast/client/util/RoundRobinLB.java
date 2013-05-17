/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client.util;

import com.hazelcast.client.LoadBalancer;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipListener;

import java.util.concurrent.atomic.AtomicLong;

public class RoundRobinLB extends AbstractLoadBalancer implements LoadBalancer, MembershipListener {

    private final AtomicLong index = new AtomicLong(0);

    public Member next() {
        final Member[] members = getMembers();
        if (members == null || members.length == 0) {
            return null;
        }
        return members[(int) (index.getAndAdd(1) % members.length)];
    }
}
