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

package com.hazelcast.map.impl;

import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializableByConvention;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.CollectionUtil;
import com.hazelcast.util.UnmodifiableIterator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_NODE;

public final class MapKeyLoaderUtil {

    private MapKeyLoaderUtil() {
    }

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

    static Iterator<Map<Integer, List<Data>>> toBatches(final Iterator<Entry<Integer, Data>> entries,
                                                        final int maxBatch) {

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
                return nextBatch(entries, maxBatch);
            }
        };
    }

    static Map<Integer, List<Data>> nextBatch(Iterator<Entry<Integer, Data>> entries, int maxBatch) {

        Map<Integer, List<Data>> batch = new HashMap<Integer, List<Data>>();

        while (entries.hasNext()) {
            Entry<Integer, Data> e = entries.next();
            List<Data> partitionKeys = CollectionUtil.addToValueList(batch, e.getKey(), e.getValue());

            if (partitionKeys.size() >= maxBatch) {
                break;
            }
        }

        return batch;
    }

    public static int getMaxSizePerNode(MaxSizeConfig maxSizeConfig) {
        // max size or -1 if policy is different or not set
        double maxSizePerNode = maxSizeConfig.getMaxSizePolicy() == PER_NODE ? maxSizeConfig.getSize() : -1D;

        if (maxSizePerNode == MaxSizeConfig.DEFAULT_MAX_SIZE) {
            // unlimited
            return -1;
        }

        return (int) maxSizePerNode;
    }

    static IFunction<Data, Entry<Integer, Data>> toPartition(final IPartitionService partitionService) {
        return new DataToEntry(partitionService);
    }

    @SerializableByConvention
    private static class DataToEntry implements IFunction<Data, Entry<Integer, Data>> {
        private final IPartitionService partitionService;

        public DataToEntry(IPartitionService partitionService) {
            this.partitionService = partitionService;
        }

        @Override
        public Entry<Integer, Data> apply(Data input) {
            Integer partition = partitionService.getPartitionId(input);
            return new MapEntrySimple<Integer, Data>(partition, input);
        }
    }

}
