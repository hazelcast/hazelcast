/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.AfterClass;
import org.junit.BeforeClass;

@SuppressWarnings("WeakerAccess")
public abstract class AbstractClientMapTest extends HazelcastTestSupport {

    protected static TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    protected static HazelcastInstance client;

    protected static HazelcastInstance member1;
    protected static HazelcastInstance member2;

    @BeforeClass
    public static final void startHazelcastInstances() {
        Config config = regularInstanceConfig();
        MapConfig mapConfig = new MapConfig("mapWithTTL");
        mapConfig.setTimeToLiveSeconds(1);
        config.addMapConfig(mapConfig);

        MapConfig mapConfig1 = new MapConfig("mapWithMaxIdle");
        mapConfig1.setMaxIdleSeconds(11);
        config.addMapConfig(mapConfig1);
        ClientConfig clientConfig = getClientConfig();

        member1 = hazelcastFactory.newHazelcastInstance(config);
        member2 = hazelcastFactory.newHazelcastInstance(config);

        client = hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @AfterClass
    public static final void stopHazelcastInstances() {
        hazelcastFactory.terminateAll();
    }

    protected static ClientConfig getClientConfig() {
        return new ClientConfig();
    }
}
