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

package com.hazelcast.test.starter.hz3;

import com.hazelcast.core.HazelcastInstance;

public class Hazelcast3Starter {

    /**
     * Creates a Hazelcast 3 member with Hazelcast 4 interface
     * It translates method calls using a Mockito based proxy.
     *
     * It is not fully tested, see {@link Hazelcast3StarterTest} for what works.
     *
     * @param xmlConfig member xml configuration (using 3.x schema!)
     *
     * @return Hazelcast instance
     */
    public static HazelcastInstance newHazelcastInstance(String xmlConfig) {
        return new Hazelcast3Proxy().newHazelcastInstance(xmlConfig);
    }

}
