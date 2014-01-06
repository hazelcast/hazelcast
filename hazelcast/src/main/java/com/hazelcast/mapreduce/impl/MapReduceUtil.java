/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.partition.PartitionService;

import java.util.*;

public final class MapReduceUtil {

    private static final String EXECUTOR_NAME_PREFIX = "mapreduce::hz::";

    private MapReduceUtil() {
    }

    public static <KeyIn> Map<Integer, List<KeyIn>> mapKeysToPartition(PartitionService ps, Collection<KeyIn> keys) {
        if (keys == null) {
            return Collections.emptyMap();
        }
        Map<Integer, List<KeyIn>> mappedKeys = new HashMap<Integer, List<KeyIn>>();
        for (KeyIn key : keys) {
            int partitionId = ps.getPartitionId(key);
            List<KeyIn> selectedKeys = mappedKeys.get(partitionId);
            if (selectedKeys == null) {
                selectedKeys = new ArrayList<KeyIn>();
                mappedKeys.put(partitionId, selectedKeys);
            }
            selectedKeys.add(key);
        }
        return mappedKeys;
    }

    public static <KeyIn> Map<Address, List<KeyIn>> mapKeysToMember(PartitionService ps, Collection<KeyIn> keys) {
        if (keys == null) {
            return Collections.emptyMap();
        }
        Map<Address, List<KeyIn>> mappedKeys = new HashMap<Address, List<KeyIn>>();
        for (KeyIn key : keys) {
            int partitionId = ps.getPartitionId(key);
            Address address = ps.getPartitionOwner(partitionId);
            List<KeyIn> selectedKeys = mappedKeys.get(address);
            if (selectedKeys == null) {
                selectedKeys = new ArrayList<KeyIn>();
                mappedKeys.put(address, selectedKeys);
            }
            selectedKeys.add(key);
        }
        return mappedKeys;
    }

    public static <K, V> Map<Address, Map<K, V>> mapResultToMember(MapReduceService mapReduceService,
                                                                   Map<K, V> result) {

        Map<Address, Map<K, V>> mapping = new HashMap<Address, Map<K, V>>();
        for (Map.Entry<K, V> entry : result.entrySet()) {
            Address address = mapReduceService.getKeyMember(entry.getKey());
            Map<K, V> data = mapping.get(address);
            if (data == null) {
                data = new HashMap<K, V>();
                mapping.put(address, data);
            }
            data.put(entry.getKey(), entry.getValue());
        }
        return mapping;
    }

    public static String buildExecutorName(String name) {
        return EXECUTOR_NAME_PREFIX + name;
    }

}
