/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.operation.steps.UtilSteps;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

/**
 * Operation to destroy the map data on the partition thread
 */
public class MapPartitionDestroyOperation extends AbstractMapLocalOperation
        implements PartitionAwareOperation, AllowedDuringPassiveState {

    private final PartitionContainer partitionContainer;
    private final MapContainer mapContainer;

    public MapPartitionDestroyOperation(PartitionContainer container, MapContainer mapContainer) {
        super(mapContainer.getName());

        this.partitionContainer = container;
        this.mapContainer = mapContainer;
        setPartitionId(partitionContainer.getPartitionId());
    }

    @Override
    protected void runInternal() {
        partitionContainer.destroyMap(mapContainer);
    }

    @Override
    public Step getStartingStep() {
        return UtilSteps.DIRECT_RUN_STEP;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }
}
