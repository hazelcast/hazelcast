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

package com.hazelcast.config;

import com.hazelcast.internal.config.TopicConfigReadOnly;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TopicConfigTest {

    /**
     * Test method for {@link com.hazelcast.config.TopicConfig#getName()}.
     */
    @Test
    public void testGetName() {
        TopicConfig topicConfig = new TopicConfig();
        assertNull(topicConfig.getName());
    }

    /**
     * Test method for {@link com.hazelcast.config.TopicConfig#setName(java.lang.String)}.
     */
    @Test
    public void testSetName() {
        TopicConfig topicConfig = new TopicConfig().setName("test");
        assertTrue("test".equals(topicConfig.getName()));
    }

    /**
     * Test method for {@link com.hazelcast.config.TopicConfig#isGlobalOrderingEnabled()}.
     */
    @Test
    public void testIsGlobalOrderingEnabled() {
        TopicConfig topicConfig = new TopicConfig();
        assertFalse(topicConfig.isGlobalOrderingEnabled());
    }

    /**
     * Test method for {@link com.hazelcast.config.TopicConfig#setGlobalOrderingEnabled(boolean)}.
     */
    @Test
    public void testSetGlobalOrderingEnabled() {
        TopicConfig topicConfig = new TopicConfig().setGlobalOrderingEnabled(true);
        assertTrue(topicConfig.isGlobalOrderingEnabled());
        try {
            topicConfig.setMultiThreadingEnabled(true);
            assertTrue("multi-threading must be disabled when global-ordering is enabled", false);
        } catch (IllegalArgumentException e) {
            // anticipated..
        }
        assertFalse(topicConfig.isMultiThreadingEnabled());
    }

    /**
     * Test method for {@link com.hazelcast.config.TopicConfig#isMultiThreadingEnabled()}.
     */
    @Test
    public void testIsMultiThreadingEnabled() {
        TopicConfig topicConfig = new TopicConfig();
        assertFalse(topicConfig.isMultiThreadingEnabled());
    }

    /**
     * Test method for {@link com.hazelcast.config.TopicConfig#setMultiThreadingEnabled(boolean)}.
     */
    @Test
    public void testSetMultiThreadingEnabled() {
        TopicConfig topicConfig = new TopicConfig().setGlobalOrderingEnabled(false);
        topicConfig.setMultiThreadingEnabled(true);
        assertTrue(topicConfig.isMultiThreadingEnabled());
        try {
            topicConfig.setGlobalOrderingEnabled(true);
            assertTrue("global-ordering must be disabled when multi-threading is enabled", false);
        } catch (IllegalArgumentException e) {
            // anticipated..
        }
        assertFalse(topicConfig.isGlobalOrderingEnabled());
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(TopicConfig.class)
                .withPrefabValues(TopicConfigReadOnly.class,
                        new TopicConfigReadOnly(new TopicConfig("Topic1")),
                        new TopicConfigReadOnly(new TopicConfig("Topic2")))
                .suppress(Warning.NONFINAL_FIELDS)
                .verify();

    }
}
