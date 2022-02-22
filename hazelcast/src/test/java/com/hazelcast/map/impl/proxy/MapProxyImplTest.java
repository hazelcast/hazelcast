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

package com.hazelcast.map.impl.proxy;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.EAGER;
import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.LAZY;
import static org.junit.Assert.assertEquals;

/**
 * Tests the creation of a map proxy.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapProxyImplTest extends HazelcastTestSupport {

    @Test
    public void whenMapProxyIsCreated_mapContainerIsNotCreated() {
        HazelcastInstance hz = createHazelcastInstance();
        MapProxyImpl mapProxy = (MapProxyImpl) hz.getMap(randomMapName());
        assertNoMapContainersExist(mapProxy);
    }

    @Test
    public void whenMapProxyWithLazyMapStoreIsCreated_mapContainerIsNotCreated() {
        String mapName = randomMapName();
        Config config = getConfigWithMapStore(mapName, LAZY);
        HazelcastInstance hz = createHazelcastInstance(config);
        MapProxyImpl mapProxy = (MapProxyImpl) hz.getMap(randomMapName());
        assertNoMapContainersExist(mapProxy);
    }

    @Test
    public void whenMapProxyWithEagerMapStoreIsCreated_mapContainerIsNotCreated() {
        String mapName = randomMapName();
        Config config = getConfigWithMapStore(mapName, EAGER);
        HazelcastInstance hz = createHazelcastInstance(config);
        MapProxyImpl mapProxy = (MapProxyImpl) hz.getMap(randomMapName());
        assertNoMapContainersExist(mapProxy);
    }

    @Test
    public void whenNearCachedMapProxyIsCreated_mapContainerIsNotCreated() {
        String mapName = randomMapName();
        Config config = new Config();
        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setName(mapName)
                .setInMemoryFormat(BINARY)
                .setInvalidateOnChange(false);
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        HazelcastInstance hz = createHazelcastInstance(config);
        MapProxyImpl mapProxy = (MapProxyImpl) hz.getMap(mapName);
        assertNoMapContainersExist(mapProxy);
    }

    private void assertNoMapContainersExist(MapProxyImpl map) {
        MapServiceContext mapServiceContext = ((MapService) map.getService()).getMapServiceContext();
        assertEquals(0, mapServiceContext.getMapContainers().size());
    }

    private Config getConfigWithMapStore(String mapName, InitialLoadMode loadMode) {
        Config config = new Config();
        config.getMapConfig(mapName)
                .getMapStoreConfig().setClassName("com.hazelcast.config.helpers.DummyMapStore")
                .setInitialLoadMode(loadMode)
                .setEnabled(true);
        return config;
    }
}
