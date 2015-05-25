/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import groovy.lang.Category;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class RecordStoreTest extends HazelcastTestSupport {

    @Test
    public void testRecordStoreResetWithoutClearingIndexes() throws Exception {
        String mapName = randomName();
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(mapName);
        MapIndexConfig indexConfig = new MapIndexConfig("name", false);
        mapConfig.addMapIndexConfig(indexConfig);
        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        IMap<Object, Object> map = hazelcastInstance.getMap(mapName);
        int key = 1;
        map.put(key, new SampleObjects.Employee("tom", 24, true, 10));
        DefaultRecordStore defaultRecordStore = getRecordStore(map, key);
        defaultRecordStore.reset(false);
        assertNull(map.get(key));
        Collection<Object> values = map.values(Predicates.equal("name", "tom"));
        assertFalse(values.isEmpty());
    }

    @Test
    public void testRecordStoreResetWithIndexesCleared() throws Exception {
        String mapName = randomName();
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(mapName);
        MapIndexConfig indexConfig = new MapIndexConfig("name", false);
        mapConfig.addMapIndexConfig(indexConfig);
        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        IMap<Object, Object> map = hazelcastInstance.getMap(mapName);
        int key = 1;
        map.put(key, new SampleObjects.Employee("tom", 24, true, 10));
        DefaultRecordStore defaultRecordStore = getRecordStore(map, key);
        defaultRecordStore.reset(true);
        assertNull(map.get(key));
        Collection<Object> values = map.values(Predicates.equal("name", "tom"));
        assertTrue(values.isEmpty());
    }


    private DefaultRecordStore getRecordStore(IMap<Object, Object> map, int key) {
        MapProxyImpl mapProxy = (MapProxyImpl) map;
        MapService mapService = (MapService) mapProxy.getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        int partitionId = partitionService.getPartitionId(key);
        PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);
        RecordStore recordStore = container.getRecordStore(map.getName());
        return (DefaultRecordStore) recordStore;
    }

}
