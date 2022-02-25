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

import com.hazelcast.core.DistributedObject;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.partition.PartitionAwareService;
import com.hazelcast.spi.impl.proxyservice.ProxyService;
import com.hazelcast.internal.partition.IPartitionLostEvent;

import java.util.Collection;

/**
 * Defines partition-aware operations' behavior of map service.
 * Currently, it only defines the behavior for partition lost occurrences
 *
 * @see IPartitionLostEvent
 */
class MapPartitionAwareService implements PartitionAwareService {

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;
    private final ProxyService proxyService;

    MapPartitionAwareService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.proxyService = this.nodeEngine.getProxyService();
    }

    @Override
    public void onPartitionLost(IPartitionLostEvent partitionLostEvent) {
        final Address thisAddress = nodeEngine.getThisAddress();
        final int partitionId = partitionLostEvent.getPartitionId();

        Collection<DistributedObject> result = proxyService.getDistributedObjects(MapService.SERVICE_NAME);

        for (DistributedObject object : result) {
            final MapProxyImpl mapProxy = (MapProxyImpl) object;
            final String mapName = mapProxy.getName();

            if (mapProxy.getTotalBackupCount() <= partitionLostEvent.getLostReplicaIndex()) {
                mapServiceContext.getMapEventPublisher().publishMapPartitionLostEvent(thisAddress, mapName, partitionId);
            }
        }
    }

}
