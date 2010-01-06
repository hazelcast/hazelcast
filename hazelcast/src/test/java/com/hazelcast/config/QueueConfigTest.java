/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.config;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 *
 */
public class QueueConfigTest {

    /**
     * Test method for {@link com.hazelcast.config.QueueConfig#getName()}.
     */
    @Test
    public void testGetName() {
        QueueConfig queueConfig = new QueueConfig();
        assertNull(null, queueConfig.getName());
    }

    /**
     * Test method for {@link com.hazelcast.config.QueueConfig#setName(java.lang.String)}.
     */
    @Test
    public void testSetName() {
        QueueConfig queueConfig = new QueueConfig().setName("a test name");
        assertEquals("a test name", queueConfig.getName());
    }

    /**
     * Test method for {@link com.hazelcast.config.QueueConfig#getMaxSizePerJVM()}.
     */
    @Test
    public void testGetMaxSizePerJVM() {
        QueueConfig queueConfig = new QueueConfig();
        assertEquals(QueueConfig.DEFAULT_MAX_SIZE_PER_JVM, queueConfig.getMaxSizePerJVM());
    }

    /**
     * Test method for {@link com.hazelcast.config.QueueConfig#setMaxSizePerJVM(int)}.
     */
    @Test
    public void testSetMaxSizePerJVM() {
        QueueConfig queueConfig = new QueueConfig().setMaxSizePerJVM(1234);
        assertEquals(1234, queueConfig.getMaxSizePerJVM());
    }

    /**
     * Test method for {@link com.hazelcast.config.QueueConfig#setMaxSizePerJVM(int)}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetMaxSizePerJVMMustBePositive() {
        new QueueConfig().setMaxSizePerJVM(-1);
    }

    /**
     * Test method for {@link com.hazelcast.config.QueueConfig#getTimeToLiveSeconds()}.
     */
    @Test
    public void testGetTimeToLiveSeconds() {
        QueueConfig queueConfig = new QueueConfig();
        assertEquals(QueueConfig.DEFAULT_TTL_SECONDS, queueConfig.getTimeToLiveSeconds());
    }

    /**
     * Test method for {@link com.hazelcast.config.QueueConfig#setTimeToLiveSeconds(int)}.
     */
    @Test
    public void testSetTimeToLiveSeconds() {
        QueueConfig queueConfig = new QueueConfig().setTimeToLiveSeconds(1234);
        assertEquals(1234, queueConfig.getTimeToLiveSeconds());
    }
}
