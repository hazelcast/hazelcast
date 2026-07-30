/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.h2.BlacklistedDummy;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DefaultDeserializationBlacklistTest extends HazelcastTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    // A dummy trigger gadget
    private static Object gadget() {
        return new BlacklistedDummy();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void testDefaultBlacklistBlocksGadget_noFilterConfig() {
        HazelcastInstance[] instances = factory.newInstances(new Config(), 2);
        String key = generateKeyOwnedBy(instances[0]);
        instances[1].getMap("test").put(key, gadget());

        assertThrows(HazelcastSerializationException.class, () -> instances[0].getMap("test").get(key));
    }

    @Test
    public void testDefaultBlacklistDisabled() {
        Config config = new Config();
        JavaSerializationFilterConfig filter = new JavaSerializationFilterConfig();
        filter.setDefaultsDisabled(true);
        config.getSerializationConfig().setJavaSerializationFilterConfig(filter);
        HazelcastInstance[] instances = factory.newInstances(config, 2);
        String key = generateKeyOwnedBy(instances[0]);
        instances[1].getMap("test").put(key, gadget());

        assertNotNull(instances[0].getMap("test").get(key));
    }

    @Test
    public void testDefaultBlacklistBlocksGadget_readOnClient() {
        HazelcastInstance member = factory.newInstances(new Config(), 1)[0];
        HazelcastInstance client = factory.newHazelcastClient(new ClientConfig());
        String key = "key";
        member.getMap("test").put(key, gadget());

        assertThrows(HazelcastSerializationException.class, () -> client.getMap("test").get(key));
    }

    @Test
    public void testDefaultBlacklistDisabledOnClient() {
        HazelcastInstance member = factory.newInstances(new Config(), 1)[0];
        ClientConfig clientConfig = new ClientConfig();
        String key = "key";
        JavaSerializationFilterConfig filter = new JavaSerializationFilterConfig();
        filter.setDefaultsDisabled(true);
        clientConfig.getSerializationConfig().setJavaSerializationFilterConfig(filter);
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        member.getMap("test").put(key, gadget());
        assertNotNull(client.getMap("test").get(key));
    }

    @Test
    public void benignClass_roundTrips_withNoFilterConfig() {
        // null filter config
        SerializationService ss = new DefaultSerializationServiceBuilder().setConfig(new SerializationConfig()).build();
        Data data = ss.toData(new example.serialization.TestDeserialized());
        // must not throw with blacklist-only default
        assertNotNull(ss.toObject(data));
    }
}
