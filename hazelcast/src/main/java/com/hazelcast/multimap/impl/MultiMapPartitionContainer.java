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

package com.hazelcast.multimap.impl;

import com.hazelcast.internal.locksupport.LockSupportService;
import com.hazelcast.internal.partition.impl.NameSpaceUtil;
import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.util.MapUtil.createConcurrentHashMap;

public class MultiMapPartitionContainer {

    final ConcurrentMap<String, MultiMapContainer> containerMap = createConcurrentHashMap(1000);

    final MultiMapService service;
    final int partitionId;

    private final ConstructorFunction<String, MultiMapContainer> containerConstructor
            = new ConstructorFunction<String, MultiMapContainer>() {
        public MultiMapContainer createNew(String name) {
            return new MultiMapContainer(name, service, partitionId);
        }
    };

    MultiMapPartitionContainer(MultiMapService service, int partitionId) {
        this.service = service;
        this.partitionId = partitionId;
    }

    public MultiMapContainer getOrCreateMultiMapContainer(String name) {
        return getOrCreateMultiMapContainer(name, true);
    }

    public MultiMapContainer getOrCreateMultiMapContainer(String name, boolean isAccess) {
        MultiMapContainer container = ConcurrencyUtil.getOrPutIfAbsent(containerMap, name, containerConstructor);
        if (isAccess) {
            container.access();
        }
        return container;
    }

    /**
     * Returns the {@link MultiMapContainer} with the given {@code name}
     * if exists or {@code null otherwise}. Depending on the {@code isAccess}
     * parameter this call updates the {@code lastAccessTime} field of the
     * container.
     *
     * @param name     The name of the container to retrieve
     * @param isAccess Indicates whether or not this call should be treated
     *                 as an access
     * @return the container or {@code null} if doesn't exist
     */
    public MultiMapContainer getMultiMapContainer(String name, boolean isAccess) {
        MultiMapContainer container = containerMap.get(name);
        if (container != null && isAccess) {
            container.access();
        }
        return container;
    }

    public Collection<ServiceNamespace> getAllNamespaces(int replicaIndex) {
        return NameSpaceUtil.getAllNamespaces(containerMap,
                container -> container.getConfig().getTotalBackupCount() >= replicaIndex,
                MultiMapContainer::getObjectNamespace);
    }

    void destroyMultiMap(String name) {
        MultiMapContainer container = containerMap.remove(name);
        if (container != null) {
            container.destroy();
        } else {
            // It can be that, multimap is used only for locking,
            // because of that MultiMapContainer is not created.
            // We will try to remove/clear LockStore belonging to
            // this MultiMap partition.
            clearLockStore(name);
        }
    }

    private void clearLockStore(String name) {
        NodeEngine nodeEngine = service.getNodeEngine();
        LockSupportService lockService = nodeEngine.getServiceOrNull(LockSupportService.SERVICE_NAME);
        if (lockService != null) {
            DistributedObjectNamespace namespace = new DistributedObjectNamespace(MultiMapService.SERVICE_NAME, name);
            lockService.clearLockStore(partitionId, namespace);
        }
    }

    void destroy() {
        for (MultiMapContainer container : containerMap.values()) {
            container.destroy();
        }
        containerMap.clear();
    }
}
