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

package com.hazelcast.cardinality.impl;

import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.merge.AbstractNamedContainerCollector;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.util.MapUtil.createConcurrentHashMap;

class CardinalityEstimatorContainerCollector extends AbstractNamedContainerCollector<CardinalityEstimatorContainer> {

    private final Config config;
    private final ConcurrentMap<CardinalityEstimatorContainer, String> containerNames;
    private final ConcurrentMap<CardinalityEstimatorContainer, MergePolicyConfig> containerPolicies;

    CardinalityEstimatorContainerCollector(NodeEngine nodeEngine,
                                           ConcurrentMap<String, CardinalityEstimatorContainer> containers) {
        super(nodeEngine, containers);
        this.config = nodeEngine.getConfig();
        this.containerNames = createConcurrentHashMap(containers.size());
        this.containerPolicies = createConcurrentHashMap(containers.size());
    }

    /**
     * The {@link CardinalityEstimatorContainer} doesn't know its name or configuration, so we create these lookup maps.
     * This is cheaper than storing this information permanently in the container.
     */
    @Override
    protected void onIteration(String containerName, CardinalityEstimatorContainer container) {
        CardinalityEstimatorConfig cardinalityEstimatorConfig = config.findCardinalityEstimatorConfig(containerName);

        containerNames.put(container, containerName);
        containerPolicies.put(container, cardinalityEstimatorConfig.getMergePolicyConfig());
    }

    public String getContainerName(CardinalityEstimatorContainer container) {
        return containerNames.get(container);
    }

    @Override
    protected MergePolicyConfig getMergePolicyConfig(CardinalityEstimatorContainer container) {
        return containerPolicies.get(container);
    }

    @Override
    protected void destroy(CardinalityEstimatorContainer container) {
    }

    @Override
    protected void destroyBackup(CardinalityEstimatorContainer container) {
    }

    @Override
    public void onDestroy() {
        containerNames.clear();
        containerPolicies.clear();
    }

    @Override
    protected int getMergingValueCount() {
        int size = 0;
        for (Collection<CardinalityEstimatorContainer> containers : getCollectedContainers().values()) {
            size += containers.size();
        }
        return size;
    }
}
