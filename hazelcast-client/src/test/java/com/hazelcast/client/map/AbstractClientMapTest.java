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

package com.hazelcast.client.map;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;

@SuppressWarnings("WeakerAccess")
public abstract class AbstractClientMapTest extends HazelcastTestSupport {

    protected final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    protected HazelcastInstance client;

    protected HazelcastInstance member1;
    protected HazelcastInstance member2;

    @Before
    public final void startHazelcastInstances() {
        Config config = getConfig();
        ClientConfig clientConfig = getClientConfig();

        member1 = hazelcastFactory.newHazelcastInstance(config);
        member2 = hazelcastFactory.newHazelcastInstance(config);

        client = hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @After
    public final void stopHazelcastInstances() {
        hazelcastFactory.terminateAll();
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }
}
