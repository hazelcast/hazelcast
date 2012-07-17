/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.map;

import com.hazelcast.config.MapConfig;
import com.hazelcast.impl.Node;
import com.hazelcast.impl.partition.PartitionInfo;

import java.util.HashMap;
import java.util.Map;

public class PartitionContainer {
    private final Node node;
    final PartitionInfo partitionInfo;
    final Map<String, MapPartition> maps = new HashMap<String, MapPartition>(100);

    public PartitionContainer(Node node, PartitionInfo partitionInfo) {
        this.node = node;
        this.partitionInfo = partitionInfo;
    }

    MapConfig getMapConfig(String name) {
        return node.getConfig().findMatchingMapConfig(name.substring(2));
    }

    public MapPartition getMapPartition(String name) {
        MapPartition mapPartition = maps.get(name);
        if (mapPartition == null) {
            mapPartition = new MapPartition(PartitionContainer.this);
            maps.put(name, mapPartition);
        }
        return mapPartition;
    }
}
