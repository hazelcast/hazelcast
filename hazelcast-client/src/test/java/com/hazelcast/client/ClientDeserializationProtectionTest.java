/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.Serializable;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestAwareClientFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Tests if deserialization blacklisting works for clients
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({ QuickTest.class, ParallelTest.class })
public class ClientDeserializationProtectionTest {

    private final TestAwareClientFactory factory = new TestAwareClientFactory();

    @After
    public void killAllHazelcastInstances() throws IOException {
        factory.terminateAll();
    }

    /**
     * <pre>
     * Given: Serialization filter is configured with a blacklist on the client.
     * When: Blacklisted class is deserialized.
     * Then: The deserialization fails.
     * </pre>
     */
    @Test
    public void test() throws Exception {
        HazelcastInstance hz = factory.newHazelcastInstance(null);
        ClientConfig config = new ClientConfig();
        config.setProperty(GroupProperty.SERIALIZATION_FILTER_ENABLED.getName(), "true");
        config.setProperty(GroupProperty.SERIALIZATION_FILTER_BLACKLIST_CLASSES.getName(), TestDeserialized.class.getName());
        HazelcastInstance client = factory.newHazelcastClient(config);

        hz.getMap("test").put("test", new TestDeserialized());
        try {
            client.getMap("test").get("test");
            fail("Deserialization should fail");
        } catch (HazelcastSerializationException s) {
            assertEquals("SecurityException was expected as a cause of failed deserialization", SecurityException.class,
                    s.getCause().getClass());
            assertFalse("Untrusted deserialization was possible", TestDeserialized.IS_DESERIALIZED);
        }
    }

    public static class TestDeserialized implements Serializable {
        private static final long serialVersionUID = 1L;
        public static volatile boolean IS_DESERIALIZED = false;

        private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        }

        private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
            IS_DESERIALIZED = true;
        }
    }
}
