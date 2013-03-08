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

package com.hazelcast.client.impl;

import com.hazelcast.client.Router;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

/**
 * A {@link Router} that always routes to a single member, no matter if that member is part of the cluster or not.
 *
 * The StaticRouter can be useful for debugging.
 */
public class StaticRouter implements Router {
    final Member member;

    StaticRouter(Member member) {
        this.member = member;
    }

    @Override
    public void init(HazelcastInstance h) {
    }

    @Override
    public Member next() {
        return member;
    }
}
