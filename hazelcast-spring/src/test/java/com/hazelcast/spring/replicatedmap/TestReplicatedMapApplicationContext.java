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

package com.hazelcast.spring.replicatedmap;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spring.CustomSpringJUnit4ClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"replicatedMap-applicationContext-hazelcast.xml"})
@Category(QuickTest.class)
public class TestReplicatedMapApplicationContext {

    @Resource(name = "instance")
    private HazelcastInstance instance;

    @BeforeClass
    @AfterClass
    public static void start() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testReplicatedMapConfig() {
        ReplicatedMapConfig replicatedMapConfig = instance.getConfig().getReplicatedMapConfig("replicatedMap");
        assertNotNull(replicatedMapConfig);
        assertEquals(3, replicatedMapConfig.getConcurrencyLevel());
        assertEquals("OBJECT", InMemoryFormat.OBJECT.name());
        assertTrue(replicatedMapConfig.isAsyncFillup());
        assertFalse(replicatedMapConfig.isStatisticsEnabled());
        assertEquals(10, replicatedMapConfig.getReplicationDelayMillis());

        List<ListenerConfig> listenerConfigs = replicatedMapConfig.getListenerConfigs();
        assertEquals(1, listenerConfigs.size());
        assertEquals("com.hazelcast.spring.DummyEntryListener", listenerConfigs.get(0).getClassName());
    }

}
