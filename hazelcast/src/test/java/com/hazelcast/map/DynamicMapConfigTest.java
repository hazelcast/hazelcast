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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.operation.UpdateMapConfigOperation;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.config.EvictionPolicy.NONE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Map configuration can be updated dynamically at runtime by using management center ui.
 * This test verifies that the changes will be reflected to corresponding IMap at runtime.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicMapConfigTest extends HazelcastTestSupport {

    @Test
    public void testMapConfigUpdate_reflectedToRecordStore() throws ExecutionException, InterruptedException {
        String mapName = randomMapName();

        Config config = getConfig();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");

        HazelcastInstance node = createHazelcastInstance(config);

        IMap<Integer, Integer> map = node.getMap(mapName);
        // trigger recordStore creation
        map.put(1, 1);

        boolean beforeUpdate = isRecordStoreExpirable(map) && isEvictionEnabled(map);
        updateMapConfig(mapName, node);
        boolean afterUpdate = isRecordStoreExpirable(map) && isEvictionEnabled(map);

        assertFalse("Before MapConfig update, RecordStore should not be expirable and evictable", beforeUpdate);
        assertTrue("RecordStore should be expirable and evictable after MapConfig update", afterUpdate);
    }

    private void updateMapConfig(String mapName, HazelcastInstance node) throws InterruptedException, ExecutionException {
        MapConfig mapConfig = createMapConfig();
        Operation updateMapConfigOperation = new UpdateMapConfigOperation(mapName, mapConfig);
        executeOperation(node, updateMapConfigOperation);
    }

    private boolean isRecordStoreExpirable(IMap map) {
        MapProxyImpl mapProxy = (MapProxyImpl) map;
        MapService mapService = (MapService) mapProxy.getService();
        MapServiceContext mapServiceContext = (MapServiceContext) mapService.getMapServiceContext();
        PartitionContainer container = mapServiceContext.getPartitionContainer(0);
        RecordStore recordStore = container.getExistingRecordStore(map.getName());
        return recordStore.isExpirable();
    }

    private boolean isEvictionEnabled(IMap map) {
        MapProxyImpl mapProxy = (MapProxyImpl) map;
        MapService mapService = (MapService) mapProxy.getService();
        MapServiceContext mapServiceContext = (MapServiceContext) mapService.getMapServiceContext();
        MapContainer mapContainer = mapServiceContext.getMapContainer(map.getName());
        EvictionPolicy evictionPolicy = mapContainer.getMapConfig().getEvictionPolicy();
        return evictionPolicy != NONE;
    }

    private MapConfig createMapConfig() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setTimeToLiveSeconds(100);
        mapConfig.setMaxIdleSeconds(22);
        mapConfig.setEvictionPolicy(EvictionPolicy.LRU);
        mapConfig.setReadBackupData(false);
        mapConfig.setBackupCount(3);
        mapConfig.setAsyncBackupCount(2);
        mapConfig.setMaxSizeConfig(new MaxSizeConfig(111, MaxSizeConfig.MaxSizePolicy.FREE_HEAP_SIZE));
        return mapConfig;
    }

    private Object executeOperation(HazelcastInstance node, Operation op) throws InterruptedException, ExecutionException {
        OperationServiceImpl operationService = getOperationService(node);
        Address address = getAddress(node);
        InternalCompletableFuture future = operationService.invokeOnTarget(MapService.SERVICE_NAME, op, address);
        return future.get();
    }
}
