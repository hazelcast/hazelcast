/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spring;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"beans-applicationContext-hazelcast.xml"})
@Category(QuickTest.class)
public class TestBeansApplicationContext extends HazelcastTestSupport {

    @BeforeClass
    @AfterClass
    public static void cleanup() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Autowired
    private ApplicationContext context;

    @Test
    public void testApplicationContext() {
        assertTrue(HazelcastClient.getAllHazelcastClients().isEmpty());

        context.getBean("map2");

        assertEquals(1, Hazelcast.getAllHazelcastInstances().size());
        assertEquals(1, HazelcastClient.getAllHazelcastClients().size());

        HazelcastInstance hazelcast = Hazelcast.getAllHazelcastInstances().iterator().next();
        assertEquals(2, hazelcast.getDistributedObjects().size());

        context.getBean("client");
        context.getBean("client");
        assertEquals(3, HazelcastClient.getAllHazelcastClients().size());
        HazelcastClientProxy client = (HazelcastClientProxy) HazelcastClient.getAllHazelcastClients().iterator().next();
        assertNull(client.getClientConfig().getManagedContext());

        HazelcastInstance instance = (HazelcastInstance) context.getBean("instance");
        assertEquals(1, Hazelcast.getAllHazelcastInstances().size());
        assertEquals(instance, Hazelcast.getAllHazelcastInstances().iterator().next());
        assertNull(instance.getConfig().getManagedContext());
    }

    @Test
    public void testPlaceHolder() {
        HazelcastInstance instance = (HazelcastInstance) context.getBean("instance");
        waitInstanceForSafeState(instance);
        Config config = instance.getConfig();
        assertEquals("spring-cluster", config.getClusterName());
        assertTrue(config.getNetworkConfig().getJoin().getTcpIpConfig().isEnabled());
        assertEquals(6, config.getMapConfig("map1").getBackupCount());
        assertFalse(config.getMapConfig("map1").isStatisticsEnabled());
        assertEquals(64, config.getNativeMemoryConfig().getSize().getValue());
    }
}
