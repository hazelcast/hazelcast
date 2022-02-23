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

package com.hazelcast.client.util;

import com.hazelcast.client.LoadBalancer;
import com.hazelcast.cluster.Member;

import java.util.Random;

/**
 * A {@link LoadBalancer} that selects a random member to route to.
 */
public class RandomLB extends AbstractLoadBalancer {

    private final Random random = new Random();

    @Override
    public Member next() {
        return nextInternal(false);
    }

    @Override
    public Member nextDataMember() {
        return nextInternal(true);
    }

    @Override
    public boolean canGetNextDataMember() {
        return true;
    }

    private Member nextInternal(boolean dataMembers) {
        Member[] members = dataMembers ? getDataMembers() : getMembers();
        if (members == null || members.length == 0) {
            return null;
        }
        int index = random.nextInt(members.length);
        return members[index];
    }
}
