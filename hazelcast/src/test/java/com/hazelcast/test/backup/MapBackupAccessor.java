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

package com.hazelcast.test.backup;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngineImpl;

import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.TestTaskExecutorUtil.runOnPartitionThread;

/**
 * Implementation of {@link BackupAccessor} for {@link IMap}.
 *
 * @param <K> type of keys
 * @param <V> type of values
 */
public class MapBackupAccessor<K, V> extends AbstractBackupAccessor<K, V> implements BackupAccessor<K, V> {

    private final String mapName;

    MapBackupAccessor(HazelcastInstance[] cluster, String mapName, int replicaIndex) {
        super(cluster, replicaIndex);
        this.mapName = mapName;
    }

    @Override
    public int size() {
        InternalPartitionService partitionService = getNode(cluster[0]).getPartitionService();
        IPartition[] partitions = partitionService.getPartitions();
        int count = 0;
        for (IPartition partition : partitions) {
            Address replicaAddress = partition.getReplicaAddress(replicaIndex);
            if (replicaAddress == null) {
                continue;
            }
            HazelcastInstance hz = getInstanceWithAddress(replicaAddress);
            NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
            MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);

            MapServiceContext context = mapService.getMapServiceContext();
            int partitionId = partition.getPartitionId();
            PartitionContainer partitionContainer = context.getPartitionContainer(partitionId);

            count += runOnPartitionThread(hz, new SizeCallable(partitionContainer), partitionId);
        }
        return count;
    }

    @Override
    public V get(K key) {
        IPartition partition = getPartitionForKey(key);
        HazelcastInstance hz = getHazelcastInstance(partition);

        Node node = getNode(hz);
        SerializationService serializationService = node.getSerializationService();
        MapService mapService = node.getNodeEngine().getService(MapService.SERVICE_NAME);
        MapServiceContext context = mapService.getMapServiceContext();
        int partitionId = partition.getPartitionId();
        PartitionContainer partitionContainer = context.getPartitionContainer(partitionId);

        return runOnPartitionThread(hz, new GetValueCallable(serializationService, partitionContainer, key), partitionId);
    }

    public Record getRecord(K key) {
        IPartition partition = getPartitionForKey(key);
        HazelcastInstance hz = getHazelcastInstance(partition);

        Node node = getNode(hz);
        SerializationService serializationService = node.getSerializationService();
        MapService mapService = node.getNodeEngine().getService(MapService.SERVICE_NAME);
        MapServiceContext context = mapService.getMapServiceContext();
        int partitionId = partition.getPartitionId();
        PartitionContainer partitionContainer = context.getPartitionContainer(partitionId);

        return runOnPartitionThread(hz, new GetRecordCallable(serializationService, partitionContainer, key), partitionId);
    }

    private class SizeCallable extends AbstractClassLoaderAwareCallable<Integer> {

        private final PartitionContainer partitionContainer;

        SizeCallable(PartitionContainer partitionContainer) {
            this.partitionContainer = partitionContainer;
        }

        @Override
        public Integer callInternal() {
            RecordStore recordStore = partitionContainer.getExistingRecordStore(mapName);
            if (recordStore == null) {
                return 0;
            }
            return recordStore.size();
        }
    }

    private class GetValueCallable extends AbstractClassLoaderAwareCallable<V> {

        private final SerializationService serializationService;
        private final PartitionContainer partitionContainer;
        private final K key;

        GetValueCallable(SerializationService serializationService, PartitionContainer partitionContainer, K key) {
            this.serializationService = serializationService;
            this.partitionContainer = partitionContainer;
            this.key = key;
        }

        @Override
        public V callInternal() {
            RecordStore recordStore = partitionContainer.getExistingRecordStore(mapName);
            if (recordStore == null) {
                return null;
            }
            Data keyData = serializationService.toData(key);
            Object o = recordStore.get(keyData, true, null);
            if (o == null) {
                return null;
            }
            return serializationService.toObject(o);
        }
    }

    private class GetRecordCallable extends AbstractClassLoaderAwareCallable<Record> {

        private final SerializationService serializationService;
        private final PartitionContainer partitionContainer;
        private final K key;

        GetRecordCallable(SerializationService serializationService, PartitionContainer partitionContainer, K key) {
            this.serializationService = serializationService;
            this.partitionContainer = partitionContainer;
            this.key = key;
        }

        @Override
        Record callInternal() throws Exception {
            RecordStore recordStore = partitionContainer.getExistingRecordStore(mapName);
            if (recordStore == null) {
                return null;
            }
            Data keyData = serializationService.toData(key);
            return recordStore.getRecord(keyData);
        }
    }

    @Override
    public String toString() {
        return "MapBackupAccessor{mapName='" + mapName
                + '\'' + "} " + super.toString();
    }
}
