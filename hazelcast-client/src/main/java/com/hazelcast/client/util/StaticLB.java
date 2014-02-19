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

import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Member;

/**
 * The StaticLB is a {@link com.hazelcast.client.LoadBalancer} that always returns the same member. This can
 * be useful for testing if you want to hit a specific member.
 */
public class StaticLB implements LoadBalancer {

    private final Member member;

    public StaticLB(Member member) {
        this.member = member;
    }

    @Override
    public void init(Cluster cluster, ClientConfig config) {
    }

    @Override
    public Member next() {
        return member;
    }
}
