/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.core;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.impl.Node;
import com.hazelcast.impl.TestUtil;
import com.hazelcast.util.Clock;
import com.hazelcast.impl.GroupProperties;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * HazelcastTest tests general behavior for one node.
 * Node is created in the beginning of the tests and the same for all test methods.
 * <p/>
 * Unit test is whiteboard'n'fast.
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class HazelcastLogTest {

    @BeforeClass
    @AfterClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        Hazelcast.shutdownAll();
    }

    @Test
    public void testDisablingSystemLogs() throws Exception {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_SYSTEM_LOG_ENABLED, "true");
        config.getGroupConfig().setName("testDisablingSystemLogs");
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        instance.getMap("map").put("key", "value");
        Node node = TestUtil.getNode(instance);
        assertTrue(node.getSystemLogService().getLogBundle().size() > 0);
        Hazelcast.shutdownAll();
        config.setProperty(GroupProperties.PROP_SYSTEM_LOG_ENABLED, "false");
        instance = Hazelcast.newHazelcastInstance(config);
        instance2 = Hazelcast.newHazelcastInstance(config);
        instance.getMap("map").put("key2", "value2");
        assertTrue(node.getSystemLogService().getLogBundle().size() == 0);
    }

}
