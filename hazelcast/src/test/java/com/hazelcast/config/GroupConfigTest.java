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

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class GroupConfigTest {

    /**
     * Test method for {@link com.hazelcast.config.GroupConfig#GroupConfig()}.
     */
    @Test
    public void testGroupConfig() {
        GroupConfig groupConfig = new GroupConfig();
        assertTrue(groupConfig.getName().equals(GroupConfig.DEFAULT_GROUP_NAME));
        assertTrue(groupConfig.getPassword().equals(GroupConfig.DEFAULT_GROUP_PASSWORD));
    }

    /**
     * Test method for {@link com.hazelcast.config.GroupConfig#GroupConfig(java.lang.String)}.
     */
    @Test
    public void testGroupConfigString() {
        GroupConfig groupConfig = new GroupConfig("abc");
        assertTrue(groupConfig.getName().equals("abc"));
        assertTrue(groupConfig.getPassword().equals(GroupConfig.DEFAULT_GROUP_PASSWORD));
    }

    /**
     * Test method for {@link com.hazelcast.config.GroupConfig#GroupConfig(java.lang.String, java.lang.String)}.
     */
    @Test
    public void testGroupConfigStringString() {
        GroupConfig groupConfig = new GroupConfig("abc", "def");
        assertTrue(groupConfig.getName().equals("abc"));
        assertTrue(groupConfig.getPassword().equals("def"));
    }

    /**
     * Test method for {@link com.hazelcast.config.GroupConfig#getName()}.
     */
    @Test
    public void testGetName() {
        GroupConfig groupConfig = new GroupConfig();
        assertTrue(groupConfig.getName().equals(GroupConfig.DEFAULT_GROUP_NAME));
    }

    /**
     * Test method for {@link com.hazelcast.config.GroupConfig#setName(java.lang.String)}.
     */
    @Test
    public void testSetName() {
        GroupConfig groupConfig = new GroupConfig().setName("abc");
        assertTrue(groupConfig.getName().equals("abc"));
    }

    /**
     * Test method for {@link com.hazelcast.config.GroupConfig#getPassword()}.
     */
    @Test
    public void testGetPassword() {
        GroupConfig groupConfig = new GroupConfig();
        assertTrue(groupConfig.getPassword().equals(GroupConfig.DEFAULT_GROUP_PASSWORD));
    }

    /**
     * Test method for {@link com.hazelcast.config.GroupConfig#setPassword(java.lang.String)}.
     */
    @Test
    public void testSetPassword() {
        GroupConfig groupConfig = new GroupConfig().setPassword("def");
        assertTrue(groupConfig.getPassword().equals("def"));
    }
}
