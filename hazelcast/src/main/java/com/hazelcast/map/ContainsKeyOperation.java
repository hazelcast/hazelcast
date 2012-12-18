/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.impl.Record;
import com.hazelcast.map.MapPartition;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.Data;
import com.hazelcast.spi.impl.AbstractNamedKeyBasedOperation;

public class ContainsKeyOperation extends AbstractNamedKeyBasedOperation {

    boolean containsKey;

    public ContainsKeyOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public ContainsKeyOperation() {
    }

    public void run() {
        MapService mapService = (MapService) getService();
        MapPartition mapPartition = mapService.getMapPartition(getPartitionId(), name);
        containsKey = mapPartition.records.containsKey(dataKey);
    }

    @Override
    public Object getResponse() {
        return containsKey;
    }

    @Override
    public String toString() {
        return "ContainsKeyOperation{" +
                '}';
    }
}
