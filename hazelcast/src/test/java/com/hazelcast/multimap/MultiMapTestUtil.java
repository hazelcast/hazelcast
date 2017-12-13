/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.multimap;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapPartitionContainer;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.multimap.impl.MultiMapValue;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastTestSupport;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("WeakerAccess")
public final class MultiMapTestUtil {

    private MultiMapTestUtil() {
    }

    /**
     * Returns the backup entries of an {@link com.hazelcast.core.MultiMap} by a given map name.
     *
     * @param instances    the {@link HazelcastInstance} array to gather the data from
     * @param multiMapName the MultiMap name
     * @param <K>          type of the key
     * @param <V>          type of the value
     * @return a {@link Map} with the backup entries
     */
    public static <K, V> Map<K, Collection<V>> getBackupMultiMap(HazelcastInstance[] instances, String multiMapName) {
        Map<K, Collection<V>> map = new HashMap<K, Collection<V>>();
        for (HazelcastInstance instance : instances) {
            NodeEngineImpl nodeEngine = HazelcastTestSupport.getNodeEngineImpl(instance);
            MultiMapService mapService = nodeEngine.getService(MultiMapService.SERVICE_NAME);
            InternalPartitionService partitionService = nodeEngine.getPartitionService();
            SerializationService serializationService = nodeEngine.getSerializationService();

            for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
                if (partitionService.getPartition(partitionId, false).isLocal()) {
                    continue;
                }
                MultiMapPartitionContainer partitionContainer = mapService.getPartitionContainer(partitionId);
                MultiMapContainer multiMapContainer = partitionContainer.getMultiMapContainer(multiMapName);
                if (multiMapContainer == null) {
                    continue;
                }
                for (Map.Entry<Data, MultiMapValue> entry : multiMapContainer.getMultiMapValues().entrySet()) {
                    K key = serializationService.toObject(entry.getKey());
                    Collection<MultiMapRecord> collection = entry.getValue().getCollection(false);
                    Collection<V> values = new ArrayList<V>(collection.size());
                    for (MultiMapRecord record : collection) {
                        V value = serializationService.toObject(record.getObject());
                        values.add(value);
                    }
                    map.put(key, values);
                }
            }
        }
        return map;
    }
}
