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

package com.hazelcast.wan.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanReplicationConfigurationTest extends HazelcastTestSupport {

    private boolean isWanReplicationEnabled;
    private boolean isWanRepublishingEnabled;

    private TestHazelcastInstanceFactory factory;
    private MapContainer mapContainer;

    @Before
    public void setUp() {
        factory = createHazelcastInstanceFactory(1);
    }

    @Test
    public void testNoWanReplication() {
        isWanReplicationEnabled = false;
        isWanRepublishingEnabled = false;
        initInstanceAndMapContainer("noWanReplication");

        assertFalse(mapContainer.isWanReplicationEnabled());
        assertFalse(mapContainer.isWanRepublishingEnabled());
        assertNull(mapContainer.getWanReplicationDelegate());
        assertNull(mapContainer.getWanMergePolicy());
        assertNull(mapContainer.getMapConfig().getWanReplicationRef());
    }

    @Test
    public void testWanReplicationAndNoWanRepublishing() {
        isWanReplicationEnabled = true;
        isWanRepublishingEnabled = false;
        initInstanceAndMapContainer("withWanReplicationOnly");

        assertTrue(mapContainer.isWanReplicationEnabled());
        assertFalse(mapContainer.isWanRepublishingEnabled());
        assertNotNull(mapContainer.getWanReplicationDelegate());
        assertNotNull(mapContainer.getWanMergePolicy());

        WanReplicationRef wanReplicationRef = mapContainer.getMapConfig().getWanReplicationRef();
        assertNotNull(wanReplicationRef);
        assertEquals(mapContainer.getWanMergePolicy().getClass().getName(), wanReplicationRef.getMergePolicyClassName());
        assertFalse(wanReplicationRef.isRepublishingEnabled());
    }

    @Test
    public void testWanReplicationAndWanRepublishing() {
        isWanReplicationEnabled = true;
        isWanRepublishingEnabled = true;
        initInstanceAndMapContainer("withWanRepublishing");

        assertTrue(mapContainer.isWanReplicationEnabled());
        assertTrue(mapContainer.isWanRepublishingEnabled());
        assertNotNull(mapContainer.getWanReplicationDelegate());
        assertNotNull(mapContainer.getWanMergePolicy());

        WanReplicationRef wanReplicationRef = mapContainer.getMapConfig().getWanReplicationRef();
        assertNotNull(wanReplicationRef);
        assertEquals(mapContainer.getWanMergePolicy().getClass().getName(), wanReplicationRef.getMergePolicyClassName());
        assertTrue(wanReplicationRef.isRepublishingEnabled());
    }

    @Override
    protected Config getConfig() {
        if (!isWanReplicationEnabled) {
            return super.getConfig();
        }

        WanCustomPublisherConfig wanCustomPublisherConfig = new WanCustomPublisherConfig()
                .setPublisherId("dummyPublisherId")
                .setClassName(WanDummyPublisher.class.getName());

        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig()
                .setName("dummyWan")
                .addCustomPublisherConfig(wanCustomPublisherConfig);

        WanReplicationRef wanRef = new WanReplicationRef()
                .setName("dummyWan")
                .setRepublishingEnabled(isWanRepublishingEnabled)
                .setMergePolicyClassName(PassThroughMergePolicy.class.getName());

        MapConfig mapConfig = new MapConfig("default")
                .setWanReplicationRef(wanRef);

        return super.getConfig()
                .addWanReplicationConfig(wanReplicationConfig)
                .addMapConfig(mapConfig);
    }

    private void initInstanceAndMapContainer(String name) {
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());
        MapProxyImpl mapProxy = (MapProxyImpl) instance.getMap(name);
        MapService mapService = (MapService) mapProxy.getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        mapContainer = mapServiceContext.getMapContainer(name);
    }
}
