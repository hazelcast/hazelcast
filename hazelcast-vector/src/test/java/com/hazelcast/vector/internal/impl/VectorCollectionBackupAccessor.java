/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.backup.AbstractBackupAccessor;
import com.hazelcast.test.backup.BackupAccessor;
import com.hazelcast.vector.VectorDocument;

import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Implementation of {@link BackupAccessor} for {@link com.hazelcast.vector.VectorCollection}.
 *
 * @param <K> type of keys
 * @param <V> type of values
 */
public class VectorCollectionBackupAccessor<K, V> extends AbstractBackupAccessor<K, VectorDocument<V>> {

    private final String name;

    public VectorCollectionBackupAccessor(HazelcastInstance[] cluster, String name, int replicaIndex) {
        super(cluster, replicaIndex);
        this.name = name;
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
            int partitionId = partition.getPartitionId();

            HazelcastInstance hz = getInstanceWithAddress(replicaAddress);
            NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
            VectorCollectionService service = nodeEngine.getService(VectorCollectionService.SERVICE_NAME);
            var storage = service.getStorageOrNull(name, partitionId);
            if (storage == null) {
                continue;
            }

            // This should be ideally be done on partition thread - see note in get(K).
            count += (int) storage.size();
        }
        return count;
    }

    @Override
    public VectorDocument<V> get(K key) {
        IPartition partition = getPartitionForKey(key);
        int partitionId = partition.getPartitionId();
        HazelcastInstance hz = getHazelcastInstance(partition);

        Node node = getNode(hz);
        SerializationService serializationService = node.getSerializationService();
        VectorCollectionService service = node.getNodeEngine().getService(VectorCollectionService.SERVICE_NAME);
        var storage = service.getStorageOrNull(name, partitionId);
        if (storage == null) {
            throw new NoSuchElementException("Storage does not exist");
        }
        // Note: this should run on partition thread for full correctness,
        // however it is currently allowed to read vector collection for other threads (as search does)
        // so for the sake of simplicity this approach is used here.
        // This may have to be changed for HD memory.

        return serializationService.toObject(storage.get(serializationService.toData(key)));
    }

    public Map<K, VectorDocument<V>> getMandatoryEntries(Collection<K> keys) {
        return keys.stream().collect(Collectors.toMap(Function.identity(), k -> {
            var value = get(k);
            assertThat(value).as("Missing key %s", k).isNotNull();
            return value;
        }));
    }

    @Override
    public String toString() {
        return "VectorCollectionBackupAccessor{mapName='" + name
                + '\'' + "} " + super.toString();
    }
}
