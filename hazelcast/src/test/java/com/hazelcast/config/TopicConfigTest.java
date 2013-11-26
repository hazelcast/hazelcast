/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
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
    }
}
