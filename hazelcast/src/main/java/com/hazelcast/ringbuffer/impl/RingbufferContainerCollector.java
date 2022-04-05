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

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.spi.impl.merge.AbstractContainerCollector;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

class RingbufferContainerCollector extends AbstractContainerCollector<RingbufferContainer> {

    private final Map<Integer, Map<ObjectNamespace, RingbufferContainer>> containers;

    RingbufferContainerCollector(NodeEngine nodeEngine, Map<Integer, Map<ObjectNamespace, RingbufferContainer>> containers) {
        super(nodeEngine);
        this.containers = containers;
    }

    @Override
    protected Iterator<RingbufferContainer> containerIterator(int partitionId) {
        Map<ObjectNamespace, RingbufferContainer> containerMap = containers.get(partitionId);
        if (containerMap == null) {
            return new EmptyIterator();
        }
        return containerMap.values().iterator();
    }

    @Override
    protected MergePolicyConfig getMergePolicyConfig(RingbufferContainer container) {
        return container.getConfig().getMergePolicyConfig();
    }

    @Override
    protected void destroy(RingbufferContainer container) {
        container.clear();
    }

    @Override
    protected void destroyBackup(RingbufferContainer container) {
        container.clear();
    }

    @Override
    protected boolean isMergeable(RingbufferContainer container) {
        String containerServiceName = container.getNamespace().getServiceName();
        return RingbufferService.SERVICE_NAME.equals(containerServiceName);
    }

    @Override
    protected int getMergingValueCount() {
        int size = 0;
        for (Collection<RingbufferContainer> containers : getCollectedContainers().values()) {
            for (RingbufferContainer container : containers) {
                size += container.size();
            }
        }
        return size;
    }
}
