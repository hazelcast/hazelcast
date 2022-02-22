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

package com.hazelcast.client.serialization;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;

import example.serialization.TestExternalizableDeserialized;

/**
 * Tests untrusted deserialization protection for Externalizables.
 *
 * <pre>
 * Given: Hazelcast member and clients are started.
 * </pre>
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ExternalizableDeserializationProtectionTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    protected static TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @AfterClass
    public static final void stopHazelcastInstances() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void killAllHazelcastInstances() throws IOException {
        hazelcastFactory.terminateAll();
        TestExternalizableDeserialized.isDeserialized = false;
    }

    @Test
    public void testExternalizableProtectedOnMember() {
        JavaSerializationFilterConfig javaSerializationFilterConfig = new JavaSerializationFilterConfig()
                .setDefaultsDisabled(true);
        javaSerializationFilterConfig.getBlacklist().addClasses(TestExternalizableDeserialized.class.getName());

        Config config = smallInstanceConfig();
        config.getSerializationConfig().setJavaSerializationFilterConfig(javaSerializationFilterConfig);
        // the index will force deserialization
        config.getMapConfig("test").addIndexConfig(new IndexConfig(IndexType.HASH, "name"));
        hazelcastFactory.newHazelcastInstance(config);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        expected.expect(HazelcastSerializationException.class);
        client.getMap("test").put("key", new TestExternalizableDeserialized());
    }

    @Test
    public void testExternalizableProtectedOnClient() {
        JavaSerializationFilterConfig javaSerializationFilterConfig = new JavaSerializationFilterConfig()
                .setDefaultsDisabled(true);
        javaSerializationFilterConfig.getBlacklist().addClasses(TestExternalizableDeserialized.class.getName());

        Config config = smallInstanceConfig();
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig1 = new ClientConfig();
        HazelcastInstance client1 = hazelcastFactory.newHazelcastClient(clientConfig1);
        client1.getMap("test").put("key", new TestExternalizableDeserialized());
        // we don't have an index on map, so the value should not be deserialized
        assertFalse(TestExternalizableDeserialized.isDeserialized);
        // deserialized on client
        client1.getMap("test").get("key");
        assertTrue(TestExternalizableDeserialized.isDeserialized);
        TestExternalizableDeserialized.isDeserialized = false;

        ClientConfig clientConfig2 = new ClientConfig();
        clientConfig2.getSerializationConfig().setJavaSerializationFilterConfig(javaSerializationFilterConfig);
        HazelcastInstance client2 = hazelcastFactory.newHazelcastClient(clientConfig2);
        expected.expect(HazelcastSerializationException.class);
        client2.getMap("test").get("key");
    }

    @Test
    public void testExternalizableUnprotected() {
        Config config = smallInstanceConfig();
        config.getMapConfig("test").addIndexConfig(new IndexConfig(IndexType.HASH, "name"));
        hazelcastFactory.newHazelcastInstance(config);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        client.getMap("test").put("key", new TestExternalizableDeserialized());
        assertTrue(TestExternalizableDeserialized.isDeserialized);
        TestExternalizableDeserialized.isDeserialized = false;
        client.getMap("test").get("key");
        assertTrue(TestExternalizableDeserialized.isDeserialized);
    }
}
