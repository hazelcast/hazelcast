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

package com.hazelcast.map.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleTestObjects;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RecordStoreTest extends HazelcastTestSupport {

    @Test
    public void testRecordStoreResetWithClearingIndexes() {
        IMap<Object, Object> map = testRecordStoreReset();
        clearIndexes(map);
        Collection<Object> values = map.values(Predicates.equal("name", "tom"));
        assertTrue(values.isEmpty());
    }

    private void clearIndexes(IMap<Object, Object> map) {
        MapServiceContext mapServiceContext = getMapServiceContext((MapProxyImpl) map);
        MapContainer mapContainer = mapServiceContext.getMapContainer(map.getName());
        for (int partitionId : mapServiceContext.getOrInitCachedMemberPartitions()) {
            mapContainer.getIndexes(partitionId).destroyIndexes();
        }
    }

    @Test
    public void testRecordStoreResetWithoutClearingIndexes() {
        IMap<Object, Object> map = testRecordStoreReset();
        Collection<Object> values = map.values(Predicates.equal("name", "tom"));
        // Although, on record store reset we don't destroy indexes, we still mark partition as
        // "unindexed" in the IndexingMutationObserver#clearGlobalIndexes. Since introduction of
        // indexes consistency check (before using them for query), we don't use incomplete indexes
        // for query. Thus, the below query ignores the indexes and uses record store to run the query.
        assertTrue(values.isEmpty());
    }

    private IMap<Object, Object> testRecordStoreReset() {
        String mapName = randomName();
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(mapName);
        IndexConfig indexConfig = new IndexConfig(IndexType.HASH, "name");
        mapConfig.addIndexConfig(indexConfig);
        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        IMap<Object, Object> map = hazelcastInstance.getMap(mapName);
        int key = 1;
        map.put(key, new SampleTestObjects.Employee("tom", 24, true, 10));
        DefaultRecordStore defaultRecordStore = getRecordStore(map, key);
        defaultRecordStore.reset();
        assertNull(map.get(key));
        return map;
    }

    private DefaultRecordStore getRecordStore(IMap<Object, Object> map, int key) {
        MapServiceContext mapServiceContext = getMapServiceContext((MapProxyImpl) map);
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        IPartitionService partitionService = nodeEngine.getPartitionService();
        int partitionId = partitionService.getPartitionId(key);
        PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);
        RecordStore recordStore = container.getRecordStore(map.getName());
        return (DefaultRecordStore) recordStore;
    }

    private MapServiceContext getMapServiceContext(MapProxyImpl map) {
        MapService mapService = (MapService) map.getService();
        return mapService.getMapServiceContext();
    }
}
