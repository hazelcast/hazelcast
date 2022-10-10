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

package com.hazelcast.table.impl;


import com.hazelcast.internal.alto.OffheapAllocator;
import com.hazelcast.internal.alto.offheapmap.OffheapMap;

import java.util.HashMap;
import java.util.Map;

public class TableManager {

    private final HashMap[] partitionMaps;
    private final OffheapMap[] offheapMaps;

    public TableManager(int partitions) {
        this.partitionMaps = new HashMap[partitions];
        for (int k = 0; k < partitions; k++) {
            partitionMaps[k] = new HashMap();
        }

        this.offheapMaps = new OffheapMap[partitions];
        for (int k = 0; k < partitions; k++) {
            offheapMaps[k] = new OffheapMap(1024, new OffheapAllocator());
        }
    }

    public Map get(int partition, StringBuffer name) {
        return partitionMaps[partition];
    }

    public OffheapMap getOffheapMap(int partition, StringBuffer name) {
        return offheapMaps[partition];
    }
}
