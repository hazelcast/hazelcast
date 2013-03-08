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

package com.hazelcast.client;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

/**
 * The Router can be used for load balancing between cluster members.
 *
 * A Router belongs to a single {@link HazelcastInstance}.
 */
public interface Router {

    /**
     * Initializes the Router with the given {@link HazelcastInstance}.
     *
     * @param h  the HazelcastInstance. This value will not be null.
     */
    public void init(HazelcastInstance h);

    /**
     * Returns the next Member to route to.
     *
     * @return the next Member to route to, or null if no member is available.
     */
    public Member next();
}