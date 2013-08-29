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

package com.hazelcast.multimap;

import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author ali 1/2/13
 */
public class CollectionPartitionContainer {

    final int partitionId;

    final MultiMapService service;

    final ConcurrentMap<String, CollectionContainer> containerMap = new ConcurrentHashMap<String, CollectionContainer>(1000);

    private final ConstructorFunction<String, CollectionContainer> collectionConstructor
            = new ConstructorFunction<String, CollectionContainer>() {
        public CollectionContainer createNew(String name) {
            return new CollectionContainer(name, service, partitionId);
        }
    };

    public CollectionPartitionContainer(MultiMapService service, int partitionId) {
        this.service = service;
        this.partitionId = partitionId;
    }

    public CollectionContainer getOrCreateCollectionContainer(String name) {
        CollectionContainer container = ConcurrencyUtil.getOrPutIfAbsent(containerMap, name, collectionConstructor);
        container.access();
        return container;
    }

    public CollectionContainer getCollectionContainer(String name){
        CollectionContainer container = containerMap.get(name);
        if (container != null){
            container.access();
        }
        return container;
    }

    // need for testing..
    public boolean containsCollection(String name) {
        return containerMap.containsKey(name);
    }

    void destroyCollection(String name) {
        final CollectionContainer container = containerMap.remove(name);
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
