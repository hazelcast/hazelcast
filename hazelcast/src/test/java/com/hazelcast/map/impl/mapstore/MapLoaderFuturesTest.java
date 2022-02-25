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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapLoaderFuturesTest extends HazelcastTestSupport {

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Test
    public void zero_remaining_loading_future_after_multiple_loadAll() {
        String mapName = "load-futures";
        int keyCountToLoad = 10;

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setImplementation(new SimpleMapLoader(keyCountToLoad, false));

        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap map = node.getMap(mapName);

        for (int i = 0; i < 3; i++) {
            map.loadAll(true);
        }

        assertEquals(keyCountToLoad, map.size());
        assertEquals(0, loadingFutureCount(mapName, node));
    }

    private static int loadingFutureCount(String mapName, HazelcastInstance node) {
        int count = 0;
        NodeEngineImpl nodeEngine = getNode(node).getNodeEngine();
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            RecordStore recordStore = mapServiceContext.getExistingRecordStore(i, mapName);
            if (recordStore != null) {
                count += ((DefaultRecordStore) recordStore).getLoadingFutures().size();
            }
        }
        return count;
    }
}
