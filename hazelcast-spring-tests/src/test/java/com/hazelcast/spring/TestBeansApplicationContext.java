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

package com.hazelcast.spring;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueueConfig;
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

import static com.hazelcast.config.EvictionPolicy.RANDOM;
import static com.hazelcast.config.MaxSizePolicy.PER_PARTITION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"beans-applicationContext-hazelcast.xml"})
@Category(QuickTest.class)
public class TestBeansApplicationContext extends HazelcastTestSupport {

    public static final String INTERNAL_JET_OBJECTS_PREFIX = "__jet.";

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
        long distributedObjectsSize = hazelcast.getDistributedObjects().stream().filter(
                distributedObject -> !distributedObject.getName().startsWith(INTERNAL_JET_OBJECTS_PREFIX)).count();
        assertEquals(2, distributedObjectsSize);

        context.getBean("client");
        context.getBean("client");
        assertEquals(3, HazelcastClient.getAllHazelcastClients().size());
        HazelcastClientProxy client = (HazelcastClientProxy) HazelcastClient.getAllHazelcastClients().iterator().next();
        assertNull(client.getClientConfig().getManagedContext());

        int batchSize = client.getClientConfig().getQueryCacheConfigs().get("").get("cache1").getBatchSize();
        assertEquals(12, batchSize);

        assertFalse(client.getClientConfig().getMetricsConfig().isEnabled());

        HazelcastInstance instance = (HazelcastInstance) context.getBean("instance");
        assertEquals(1, Hazelcast.getAllHazelcastInstances().size());
        assertEquals(instance, Hazelcast.getAllHazelcastInstances().iterator().next());
        assertNull(instance.getConfig().getManagedContext());
    }

    @Test
    public void testPlaceholderInEviction() {
        assertTrue(HazelcastClient.getAllHazelcastClients().isEmpty());

        assertEquals(1, Hazelcast.getAllHazelcastInstances().size());

        HazelcastInstance instance = (HazelcastInstance) context.getBean("instance");
        MapConfig mapConfig = instance.getConfig().getMapConfig("map-with-eviction");

        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        assertThat(evictionConfig.getSize()).isEqualTo(1983);
        assertThat(evictionConfig.getMaxSizePolicy()).isEqualTo(PER_PARTITION);
        assertThat(evictionConfig.getEvictionPolicy()).isEqualTo(RANDOM);
    }

    @Test
    public void testPlaceHolder() {
        HazelcastInstance instance = (HazelcastInstance) context.getBean("instance");
        waitInstanceForSafeState(instance);
        Config config = instance.getConfig();
        assertEquals("spring-cluster", config.getClusterName());
        assertFalse(config.getNetworkConfig().getJoin().getTcpIpConfig().isEnabled());
        assertEquals(6, config.getMapConfig("map1").getBackupCount());
        assertFalse(config.getMapConfig("map1").isStatisticsEnabled());
        assertEquals(64, config.getNativeMemoryConfig().getSize().getValue());
        QueueConfig testQueue = config.getQueueConfig("testQueue");
        assertEquals("com.hazelcast.collection.impl.queue.model.PriorityElementComparator",
                testQueue.getPriorityComparatorClassName());
    }
}
