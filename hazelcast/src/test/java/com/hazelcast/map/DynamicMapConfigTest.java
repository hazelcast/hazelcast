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

package com.hazelcast.map;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.operation.UpdateMapConfigOperation;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.ChangeLoggingRule;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.EvictionPolicy.NONE;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getOperationService;
import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Map configuration can be updated dynamically at runtime by using management center ui.
 * This test verifies that the changes will be reflected to corresponding IMap at runtime.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicMapConfigTest extends HazelcastTestSupport {

    @ClassRule
    public static ChangeLoggingRule changeLoggingRule = new ChangeLoggingRule("log4j2-trace-dynamic-map-config-update.xml");

    @Test
    public void testMapConfigUpdate_reflectedToRecordStore() {
        String mapName = randomMapName();

        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");

        HazelcastInstance node = createHazelcastInstance(config);

        IMap<Integer, Integer> map = node.getMap(mapName);
        // trigger recordStore creation
        map.put(1, 1);

        assertFalse("RecordStore must not be evictable before the map config update", isEvictionEnabled(map));
        assertFalse("RecordStore must not be expirable before the map config update", isRecordStoreExpirable(map));

        updateMapConfig(mapName, node);
        // trigger recordStore expiry system, only added/updated
        // entries after config update will be affected.
        map.put(1, 1);

        assertTrue("RecordStore must be evictable after MapConfig update", isEvictionEnabled(map));
        assertTrue("RecordStore must be expirable after MapConfig update", isRecordStoreExpirable(map));
    }

    private void updateMapConfig(String mapName, HazelcastInstance node) {
        MapConfig mapConfig = createMapConfig();
        Operation updateMapConfigOperation = new UpdateMapConfigOperation(
                mapName,
                mapConfig.getTimeToLiveSeconds(),
                mapConfig.getMaxIdleSeconds(),
                mapConfig.getEvictionConfig().getSize(),
                mapConfig.getEvictionConfig().getMaxSizePolicy().getId(),
                mapConfig.isReadBackupData(),
                mapConfig.getEvictionConfig().getEvictionPolicy().getId());
        executeOperation(node, updateMapConfigOperation);
    }

    private boolean isRecordStoreExpirable(IMap map) {
        MapProxyImpl mapProxy = (MapProxyImpl) map;
        MapService mapService = (MapService) mapProxy.getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        PartitionContainer container = mapServiceContext.getPartitionContainer(0);
        RecordStore recordStore = container.getExistingRecordStore(map.getName());
        return requireNonNull(recordStore).isExpirable();
    }

    private boolean isEvictionEnabled(IMap map) {
        MapProxyImpl mapProxy = (MapProxyImpl) map;
        MapService mapService = (MapService) mapProxy.getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapContainer mapContainer = mapServiceContext.getMapContainer(map.getName());
        EvictionPolicy evictionPolicy = mapContainer.getMapConfig().getEvictionConfig().getEvictionPolicy();
        return evictionPolicy != NONE;
    }

    private MapConfig createMapConfig() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setTimeToLiveSeconds(100);
        mapConfig.setMaxIdleSeconds(22);
        mapConfig.setReadBackupData(false);
        mapConfig.setBackupCount(3);
        mapConfig.setAsyncBackupCount(2);
        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig.setEvictionPolicy(EvictionPolicy.LRU);
        evictionConfig.setSize(111).setMaxSizePolicy(MaxSizePolicy.FREE_HEAP_SIZE);
        return mapConfig;
    }

    private void executeOperation(HazelcastInstance node, Operation op) {
        OperationServiceImpl operationService = getOperationService(node);
        Address address = getAddress(node);
        operationService.invokeOnTarget(MapService.SERVICE_NAME, op, address).join();
    }
}
