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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;

import java.io.IOException;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

public class MapPartitionDestroyOperationFactory
        extends PartitionAwareOperationFactory {

    private MapContainer mapContainer;
    private MapServiceContext mapServiceContext;

    MapPartitionDestroyOperationFactory() {
        // no-op, only used locally
    }

    public MapPartitionDestroyOperationFactory(MapContainer mapContainer,
                                               MapServiceContext mapServiceContext) {
        this.mapContainer = mapContainer;
        this.mapServiceContext = mapServiceContext;
    }

    @Override
    public Operation createPartitionOperation(int partition) {
        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partition);
        return new MapPartitionDestroyOperation(partitionContainer, mapContainer)
                .setPartitionId(partition)
                .setServiceName(SERVICE_NAME);
    }

    @Override
    public int getFactoryId() {
        throw new UnsupportedOperationException(getClass().getName() + " is only used locally!");
    }

    @Override
    public int getClassId() {
        throw new UnsupportedOperationException(getClass().getName() + " is only used locally!");
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException(getClass().getName() + " is only used locally!");
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException(getClass().getName() + " is only used locally!");
    }
}
