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

package com.hazelcast.multimap.impl;

import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MultiMapPartitionContainer {

    final int partitionId;

    final MultiMapService service;

    final ConcurrentMap<String, MultiMapContainer> containerMap = new ConcurrentHashMap<String, MultiMapContainer>(1000);

    private final ConstructorFunction<String, MultiMapContainer> containerConstructor
            = new ConstructorFunction<String, MultiMapContainer>() {
        public MultiMapContainer createNew(String name) {
            return new MultiMapContainer(name, service, partitionId);
        }
    };

    public MultiMapPartitionContainer(MultiMapService service, int partitionId) {
        this.service = service;
        this.partitionId = partitionId;
    }

    public MultiMapContainer getOrCreateMultiMapContainer(String name) {
        MultiMapContainer container = ConcurrencyUtil.getOrPutIfAbsent(containerMap, name, containerConstructor);
        container.access();
        return container;
    }

    public MultiMapContainer getCollectionContainer(String name) {
        MultiMapContainer container = containerMap.get(name);
        if (container != null) {
            container.access();
        }
        return container;
    }

    // need for testing..
    public boolean containsCollection(String name) {
        return containerMap.containsKey(name);
    }

    void destroyCollection(String name) {
        final MultiMapContainer container = containerMap.remove(name);
        if (container != null) {
            container.destroy();
        }
    }

    void destroy() {
        for (MultiMapContainer container : containerMap.values()) {
            container.destroy();
        }
        containerMap.clear();
    }

}
