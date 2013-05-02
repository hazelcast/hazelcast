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

package com.hazelcast.collection;

import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @ali 1/2/13
 */
public class CollectionPartitionContainer {

    final int partitionId;

    final CollectionService service;

    final ConcurrentMap<CollectionProxyId, CollectionContainer> containerMap = new ConcurrentHashMap<CollectionProxyId, CollectionContainer>(1000);

    private final ConstructorFunction<CollectionProxyId, CollectionContainer> collectionConstructor
            = new ConstructorFunction<CollectionProxyId, CollectionContainer>() {
        public CollectionContainer createNew(CollectionProxyId proxyId) {
            return new CollectionContainer(proxyId, service, partitionId);
        }
    };

    public CollectionPartitionContainer(CollectionService service, int partitionId) {
        this.service = service;
        this.partitionId = partitionId;
    }

    public CollectionContainer getOrCreateCollectionContainer(CollectionProxyId proxyId) {
        CollectionContainer container = ConcurrencyUtil.getOrPutIfAbsent(containerMap, proxyId, collectionConstructor);
        container.access();
        return container;
    }

    public CollectionContainer getCollectionContainer(CollectionProxyId proxyId){
        CollectionContainer container = containerMap.get(proxyId);
        if (container != null){
            container.access();
        }
        return container;
    }

    void destroyCollection(CollectionProxyId collectionProxyId) {
        final CollectionContainer container = containerMap.remove(collectionProxyId);
        if (container != null) {
            container.destroy();
        }
    }

    void destroy() {
        for (CollectionContainer container : containerMap.values()) {
            container.destroy();
        }
        containerMap.clear();
    }

}
