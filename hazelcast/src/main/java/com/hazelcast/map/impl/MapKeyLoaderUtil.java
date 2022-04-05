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

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.CollectionUtil;
import com.hazelcast.internal.util.UnmodifiableIterator;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

import static com.hazelcast.config.MaxSizePolicy.PER_NODE;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public final class MapKeyLoaderUtil {

    private MapKeyLoaderUtil() {
    }

    /**
     * Returns the role for the map key loader based on the passed parameters.
     * The partition owner of the map name partition is the sender.
     * The first replica of the map name partition is the sender backup.
     * Other partition owners are receivers and other partition replicas do
     * not have a role.
     *
     * @param isPartitionOwner               if this is the partition owner
     * @param isMapNamePartition             if this is the partition containing the map name
     * @param isMapNamePartitionFirstReplica if this is the first replica for the partition
     *                                       containing the map name
     * @return the map key loader role
     */
    static MapKeyLoader.Role assignRole(boolean isPartitionOwner, boolean isMapNamePartition,
                                        boolean isMapNamePartitionFirstReplica) {
        if (isMapNamePartition) {
            if (isPartitionOwner) {
                // map-name partition owner is the SENDER
                return MapKeyLoader.Role.SENDER;
            } else {
                if (isMapNamePartitionFirstReplica) {
                    // first replica of the map-name partition is the SENDER_BACKUP
                    return MapKeyLoader.Role.SENDER_BACKUP;
                } else {
                    // other replicas of the map-name partition do not have a role
                    return MapKeyLoader.Role.NONE;
                }
            }
        } else {
            // ordinary partition owners are RECEIVERs, otherwise no role
            return isPartitionOwner ? MapKeyLoader.Role.RECEIVER : MapKeyLoader.Role.NONE;
        }
    }

    /**
     * Transforms an iterator of entries to an iterator of entry
     * batches where each batch is represented as a map from
     * entry key to list of entry values. The maximum size of the
     * entry value list in any batch is determined by the {@code
     * maxBatch} parameter. Only one entry value list may have
     * the {@code maxBatch} size, other lists will be smaller.
     *
     * @param entries                  the entries to be batched
     * @param maxBatch                 the maximum size of an entry group in a single
     *                                 batch
     * @param nodeWideLoadedKeyLimiter controls the loaded number of keys
     * @return an iterator with entry batches
     */
    static Iterator<Map<Integer, List<Data>>> toBatches(final Iterator<Entry<Integer, Data>> entries,
                                                        final int maxBatch, Semaphore nodeWideLoadedKeyLimiter) {
        return new UnmodifiableIterator<Map<Integer, List<Data>>>() {
            @Override
            public boolean hasNext() {
                return entries.hasNext();
            }

            @Override
            public Map<Integer, List<Data>> next() {
                if (!entries.hasNext()) {
                    throw new NoSuchElementException();
                }
                return nextBatch(entries, maxBatch, nodeWideLoadedKeyLimiter);
            }
        };
    }

    /**
     * Groups entries by the entry key. The entries will be grouped
     * until at least one group has up to {@code maxBatch}
     * entries or until the {@code entries} have been exhausted.
     *
     * @param entries                  the entries to be grouped by key
     * @param maxBatch                 the maximum size of a group
     * @param nodeWideLoadedKeyLimiter controls the loaded number of keys per node
     * @return the grouped entries by entry key
     */
    private static Map<Integer, List<Data>> nextBatch(Iterator<Entry<Integer, Data>> entries,
                                                      int maxBatch, Semaphore nodeWideLoadedKeyLimiter) {
        Map<Integer, List<Data>> batch = createHashMap(maxBatch);
        while (entries.hasNext()) {
            if (!nodeWideLoadedKeyLimiter.tryAcquire()) {
                break;
            }

            Entry<Integer, Data> e = entries.next();
            List<Data> partitionKeys = CollectionUtil.addToValueList(batch, e.getKey(), e.getValue());

            if (partitionKeys.size() >= maxBatch) {
                break;
            }
        }
        return batch;
    }

    /**
     * Returns the configured maximum entry count per node if the max
     * size policy is {@link MaxSizePolicy#PER_NODE}
     * and is not the default, otherwise returns {@code -1}.
     *
     * @param evictionConfig eviction config
     * @return the max size per node or {@code
     * -1} if not configured or is the default
     */
    public static int getMaxSizePerNode(EvictionConfig evictionConfig) {
        // max size or -1 if policy is different or not set
        double maxSizePerNode = evictionConfig.getMaxSizePolicy() == PER_NODE
                ? evictionConfig.getSize() : -1D;

        if (maxSizePerNode == MapConfig.DEFAULT_MAX_SIZE) {
            // unlimited
            return -1;
        }

        return (int) maxSizePerNode;
    }

    /**
     * Returns a {@link Function} that transforms a {@link Data}
     * parameter to an map entry where the key is the partition ID
     * and the value is the provided parameter.
     *
     * @param partitionService the partition service
     */
    static Function<Data, Entry<Integer, Data>> toPartition(IPartitionService partitionService) {
        return new DataToEntry(partitionService);
    }

    private static class DataToEntry implements Function<Data, Entry<Integer, Data>> {
        private final IPartitionService partitionService;

        DataToEntry(IPartitionService partitionService) {
            this.partitionService = partitionService;
        }

        @Override
        public Entry<Integer, Data> apply(Data input) {
            // Null-pointer here, in case of null key loaded by MapLoader
            checkNotNull(input, "Key loaded by a MapLoader cannot be null.");
            Integer partition = partitionService.getPartitionId(input);
            return new MapEntrySimple<>(partition, input);
        }
    }

}
