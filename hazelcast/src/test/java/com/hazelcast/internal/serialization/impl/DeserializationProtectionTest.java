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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.config.ClassFilter;
import com.hazelcast.config.Config;
import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.TestDeserialized;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests untrusted deserialization protection.
 *
 * <pre>
 * Given: 2 node cluster is started.
 * </pre>
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DeserializationProtectionTest extends HazelcastTestSupport {

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        TestDeserialized.isDeserialized = false;
    }

    /**
     * Test default filter configuration.
     *
     * <pre>
     * When: An untrusted serialized object is stored on target Hazelcast instance and default Whitelist is used.
     * Then: Deserialization fails.
     * </pre>
     */
    @Test
    public void testDefaultDeserializationFilter_keyNotOwnedByTarget() {
        assertDeserializationFails(new JavaSerializationFilterConfig(), true);
    }

    /**
     * <pre>
     * When: An untrusted serialized object is stored on source Hazelcast instance and default Whitelist is used.
     * Then: Deserialization fails.
     * </pre>
     */
    @Test
    public void testDefaultDeserializationFilter_keyOwnedBySource() {
        assertDeserializationFails(new JavaSerializationFilterConfig(), false);
    }

    /**
     * <pre>
     * When: Default Whitelist is disabled and classname of the test serialized object is blacklisted.
     * Then: Deserialization fails.
     * </pre>
     */
    @Test
    public void testClassBlacklisted() {
        ClassFilter blacklist = new ClassFilter().addClasses(TestDeserialized.class.getName());
        JavaSerializationFilterConfig filterConfig = new JavaSerializationFilterConfig().setDefaultsDisabled(true)
                .setBlacklist(blacklist);
        assertDeserializationFails(filterConfig, false);
    }

    /**
     * <pre>
     * When: Deserialization filtering is not explicitly enabled.
     * Then: Untrusted deserialization is possible.
     * </pre>
     */
    @Test
    public void testNoDeserializationFilter() {
        assertDeserializationPass(null);
    }

    /**
     * <pre>
     * When: Deserialization filtering is enabled and classname of test object is whitelisted.
     * Then: The deserialization is possible.
     * </pre>
     */
    @Test
    public void testClassWhitelisted() {
        JavaSerializationFilterConfig filterConfig = new JavaSerializationFilterConfig();
        filterConfig.getWhitelist().addClasses(TestDeserialized.class.getName());
        assertDeserializationPass(filterConfig);
    }

    private void assertDeserializationFails(JavaSerializationFilterConfig javaSerializationFilterConfig,
            boolean keyOwnedByTarget) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        Config config = new Config();
        config.getSerializationConfig().setJavaSerializationFilterConfig(javaSerializationFilterConfig);
        HazelcastInstance[] instances = factory.newInstances(config);
        String key = generateKeyOwnedBy(instances[keyOwnedByTarget ? 1 : 0]);
        instances[0].getMap("test").put(key, new TestDeserialized());
        try {
            instances[1].getMap("test").get(key);
            fail("Deserialization should have failed");
        } catch (HazelcastSerializationException e) {
            assertFalse(TestDeserialized.isDeserialized);
        }
    }

    private void assertDeserializationPass(JavaSerializationFilterConfig filterConfig) {
        Config config = new Config();
        config.getSerializationConfig().setJavaSerializationFilterConfig(filterConfig);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances(config);
        instances[0].getMap("test").put("a", new TestDeserialized());
        assertNotNull(instances[1].getMap("test").get("a"));
        assertTrue(TestDeserialized.isDeserialized);
    }

}
