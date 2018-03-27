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

package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.concurrent.atomiclong.AtomicLongContainer;
import com.hazelcast.config.AtomicReferenceConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.merge.AbstractNamedContainerCollector;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.MapUtil.createConcurrentHashMap;

class AtomicReferenceContainerCollector extends AbstractNamedContainerCollector<AtomicReferenceContainer> {

    private final Config config;
    private final ConcurrentMap<AtomicReferenceContainer, String> containerNames;
    private final ConcurrentMap<AtomicReferenceContainer, MergePolicyConfig> containerPolicies;

    AtomicReferenceContainerCollector(NodeEngine nodeEngine, ConcurrentMap<String, AtomicReferenceContainer> containers) {
        super(nodeEngine, containers);
        this.config = nodeEngine.getConfig();
        this.containerNames = createConcurrentHashMap(containers.size());
        this.containerPolicies = createConcurrentHashMap(containers.size());
    }

    /**
     * The {@link AtomicLongContainer} doesn't know its name or configuration, so we create these lookup maps.
     * This is cheaper than storing this information permanently in the container.
     */
    @Override
    protected void onIteration(String containerName, AtomicReferenceContainer container) {
        AtomicReferenceConfig atomicReferenceConfig = config.findAtomicReferenceConfig(containerName);

        containerNames.put(container, containerName);
        containerPolicies.put(container, atomicReferenceConfig.getMergePolicyConfig());
    }

    public String getContainerName(AtomicReferenceContainer container) {
        return containerNames.get(container);
    }

    @Override
    protected MergePolicyConfig getMergePolicyConfig(AtomicReferenceContainer container) {
        return containerPolicies.get(container);
    }

    @Override
    protected void destroy(AtomicReferenceContainer container) {
        container.set(null);
    }

    @Override
    protected void destroyBackup(AtomicReferenceContainer container) {
        container.set(null);
    }

    @Override
    public void onDestroy() {
        containerNames.clear();
        containerPolicies.clear();
    }

    @Override
    protected int getMergingValueCount() {
        int size = 0;
        for (Collection<AtomicReferenceContainer> containers : getCollectedContainers().values()) {
            size += containers.size();
        }
        return size;
    }
}
