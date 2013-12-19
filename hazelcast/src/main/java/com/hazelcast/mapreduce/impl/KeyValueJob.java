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

import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;

import java.util.*;

public class KeyValueJob<KeyIn, ValueIn> extends AbstractJob<KeyIn, ValueIn> {

    private final NodeEngine nodeEngine;

    public KeyValueJob(String name, NodeEngine nodeEngine, KeyValueSource<KeyIn, ValueIn> keyValueSource) {
        super(name, keyValueSource);
        this.nodeEngine = nodeEngine;
    }

    @Override
    protected void invokeTask() throws Exception {
        OperationService os = nodeEngine.getOperationService();
        PartitionService ps = nodeEngine.getPartitionService();
        SerializationService ss = nodeEngine.getSerializationService();

        Map<Integer, List<KeyIn>> mappedKeys = mapKeys(ps, keys);

    }

    private Map<Integer, List<KeyIn>> mapKeys(PartitionService ps, Collection<KeyIn> keys) {
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

}
