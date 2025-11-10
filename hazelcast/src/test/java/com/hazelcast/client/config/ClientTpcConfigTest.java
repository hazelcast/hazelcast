/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.config;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Needs to run serially because it messes with system properties.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientTpcConfigTest {

    @Before
    public void before() {
        System.clearProperty("hazelcast.client.tpc.enabled");
        System.clearProperty("hazelcast.client.tpc.connectionCount");
    }

    @After
    public void after() {
        System.clearProperty("hazelcast.client.tpc.enabled");
        System.clearProperty("hazelcast.client.tpc.connectionCount");
    }

    @Test
    public void test_constructor_enabledDefaultBehavior() {
        ClientTpcConfig config = new ClientTpcConfig();
        assertFalse(config.isEnabled());

        System.setProperty("hazelcast.client.tpc.enabled", "true");
        config = new ClientTpcConfig();
        assertTrue(config.isEnabled());

        System.setProperty("hazelcast.client.tpc.enabled", "false");
        config = new ClientTpcConfig();
        assertFalse(config.isEnabled());
    }

    @Test
    public void test_constructor_connectionCountDefaultBehavior() {
        ClientTpcConfig config = new ClientTpcConfig();
        assertEquals(1, config.getConnectionCount());

        System.setProperty("hazelcast.client.tpc.connectionCount", "5");
        config = new ClientTpcConfig();
        assertEquals(5, config.getConnectionCount());
    }

    @Test
    public void test_setConnectionCount_whenNegative() {
        ClientTpcConfig config = new ClientTpcConfig();
        assertThrows(IllegalArgumentException.class, () -> config.setConnectionCount(-1));
    }

    @Test
    public void test_setConnectionCount_whenAcceptable() {
        ClientTpcConfig config = new ClientTpcConfig();

        config.setConnectionCount(0);
        assertEquals(0, config.getConnectionCount());

        config.setConnectionCount(10);
        assertEquals(10, config.getConnectionCount());
    }
}
