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

package com.hazelcast.internal.nearcache.impl.invalidation;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StaleReadDetectorTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "test";

    @Test
    public void no_repairing_handler_created_when_invalidations_disabled() throws Exception {
        Config config = createConfigWithNearCache(false);
        HazelcastInstance node = createHazelcastInstance(config);

        IMap map = node.getMap(MAP_NAME);

        map.put(1, 1);

        map.get(1);

        assertFalse(isRepairingHandlerCreatedForMap(node, MAP_NAME));
    }

    @Test
    public void repairing_handler_created_when_invalidations_enabled() throws Exception {
        Config config = createConfigWithNearCache(true);
        HazelcastInstance node = createHazelcastInstance(config);

        IMap map = node.getMap(MAP_NAME);

        map.put(1, 1);

        map.get(1);

        assertTrue(isRepairingHandlerCreatedForMap(node, MAP_NAME));
    }

    private static boolean isRepairingHandlerCreatedForMap(HazelcastInstance node, String mapName) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(node);
        MapService mapService = nodeEngineImpl.getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        ConcurrentMap<String, RepairingHandler> handlers = mapNearCacheManager.getRepairingTask().getHandlers();
        RepairingHandler repairingHandler = handlers.get(mapName);
        return repairingHandler != null;
    }

    private Config createConfigWithNearCache(boolean invalidateOnChange) {
        Config config = getConfig();
        config.setProperty(PARTITION_COUNT.getName(), "1");

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setCacheLocalEntries(true);
        nearCacheConfig.setInvalidateOnChange(invalidateOnChange);

        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        mapConfig.setNearCacheConfig(nearCacheConfig);

        return config;
    }
}
