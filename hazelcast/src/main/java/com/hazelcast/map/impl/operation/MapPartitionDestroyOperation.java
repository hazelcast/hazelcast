/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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


import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.tenantcontrol.TenantControl;

import java.util.logging.Level;

/**
 * Operation to destroy the map data on the partition thread
 */
public class MapPartitionDestroyOperation extends AbstractMapLocalOperation
        implements PartitionAwareOperation, AllowedDuringPassiveState {

    private final PartitionContainer partitionContainer;

    public MapPartitionDestroyOperation(PartitionContainer partitionContainer,
                                        MapContainer mapContainer) {
        super(mapContainer.getName());
        this.createRecordStoreOnDemand = false;
        this.partitionContainer = partitionContainer;
        this.mapContainer = mapContainer;
    }

    @Override
    protected void runInternal() {
        if (mapContainer == null) {
            // no such map exists
            return;
        }
        partitionContainer.destroyMap(mapContainer);
    }

    @Override
    public void logError(Throwable e) {
        if (e instanceof DistributedObjectDestroyedException) {
            ILogger logger = getLogger();
            logger.log(Level.FINEST, e.getMessage());
        } else {
            super.logError(e);
        }
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    public TenantControl getTenantControl() {
        return TenantControl.NOOP_TENANT_CONTROL;
    }
}
