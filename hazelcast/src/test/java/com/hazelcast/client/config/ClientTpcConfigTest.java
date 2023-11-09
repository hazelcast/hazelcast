/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Needs to run serially because it messes with system properties.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientTpcConfigTest {

    @Before
    public void before() {
        System.clearProperty("hazelcast.client.tpc.enabled");
    }

    @After
    public void after() {
        System.clearProperty("hazelcast.client.tpc.enabled");
    }

    @Test
    public void test() {
        ClientTpcConfig config = new ClientTpcConfig();
        assertFalse(config.isEnabled());

        System.setProperty("hazelcast.client.tpc.enabled", "true");
        config = new ClientTpcConfig();
        assertTrue(config.isEnabled());

        System.setProperty("hazelcast.client.tpc.enabled", "false");
        config = new ClientTpcConfig();
        assertFalse(config.isEnabled());
    }
}
