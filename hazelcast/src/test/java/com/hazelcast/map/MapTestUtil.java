/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.util.Clock;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public final class MapTestUtil {

    private MapTestUtil() {
    }

    /**
     * Returns the backup entries of an {@link com.hazelcast.core.IMap} by a given map name.
     *
     * @param instances the {@link HazelcastInstance} array to gather the data from
     * @param mapName   the map name
     * @param <K>       type of the key
     * @param <V>       type of the value
     * @return a {@link Map} with the backup entries
     */
    public static <K, V> Map<K, V> getBackupMap(HazelcastInstance[] instances, String mapName) {
        long now = Clock.currentTimeMillis();
        Map<K, V> map = new HashMap<K, V>();
        for (HazelcastInstance instance : instances) {
            NodeEngineImpl nodeEngine = HazelcastTestSupport.getNodeEngineImpl(instance);
            MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
            MapServiceContext context = mapService.getMapServiceContext();
            InternalPartitionService partitionService = nodeEngine.getPartitionService();
            SerializationService serializationService = nodeEngine.getSerializationService();

            for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
                if (partitionService.getPartition(partitionId, false).isLocal()) {
                    continue;
                }
                PartitionContainer partitionContainer = context.getPartitionContainer(partitionId);
                //noinspection unchecked
                RecordStore<Record> recordStore = partitionContainer.getExistingRecordStore(mapName);
                if (recordStore == null) {
                    continue;
                }
                Iterator<Record> iterator = recordStore.iterator(now, false);
                while (iterator.hasNext()) {
                    Record record = iterator.next();
                    K key = serializationService.toObject(record.getKey());
                    V value = serializationService.toObject(record.getValue());
                    map.put(key, value);
                }
            }
        }
        return map;
    }
}
