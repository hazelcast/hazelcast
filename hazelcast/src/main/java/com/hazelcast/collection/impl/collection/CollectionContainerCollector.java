/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.collection;

import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.merge.AbstractNamedContainerCollector;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

class CollectionContainerCollector extends AbstractNamedContainerCollector<CollectionContainer> {

    CollectionContainerCollector(NodeEngine nodeEngine, ConcurrentMap<String, CollectionContainer> containers) {
        super(nodeEngine, containers);
    }

    @Override
    protected MergePolicyConfig getMergePolicyConfig(CollectionContainer container) {
        return container.getConfig().getMergePolicyConfig();
    }

    @Override
    protected void destroy(CollectionContainer container) {
        // owned data is stored in the collection
        container.getCollection().clear();
    }

    @Override
    protected void destroyBackup(CollectionContainer container) {
        // backup data is stored in the map
        container.getMap().clear();
    }

    @Override
    protected int getMergingValueCount() {
        int size = 0;
        for (Collection<CollectionContainer> containers : getCollectedContainers().values()) {
            for (CollectionContainer container : containers) {
                size += container.size();
            }
        }
        return size;
    }
}
