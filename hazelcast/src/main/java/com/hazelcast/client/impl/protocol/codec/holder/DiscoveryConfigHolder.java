/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.client.impl.protocol.codec.holder;

import com.hazelcast.internal.serialization.Data;

import java.util.List;
import java.util.Objects;

public final class DiscoveryConfigHolder {
    private final List<DiscoveryStrategyConfigHolder> discoveryStrategyConfigs;
    private final Data discoveryServiceProvider;
    private final Data nodeFilter;
    private final String nodeFilterClass;

    public DiscoveryConfigHolder(List<DiscoveryStrategyConfigHolder> discoveryStrategyConfigs, Data discoveryServiceProvider,
                                 Data nodeFilter, String nodeFilterClass) {
        this.discoveryStrategyConfigs = discoveryStrategyConfigs;
        this.discoveryServiceProvider = discoveryServiceProvider;
        this.nodeFilter = nodeFilter;
        this.nodeFilterClass = nodeFilterClass;
    }

    public List<DiscoveryStrategyConfigHolder> getDiscoveryStrategyConfigs() {
        return discoveryStrategyConfigs;
    }

    public Data getDiscoveryServiceProvider() {
        return discoveryServiceProvider;
    }

    public Data getNodeFilter() {
        return nodeFilter;
    }

    public String getNodeFilterClass() {
        return nodeFilterClass;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DiscoveryConfigHolder that = (DiscoveryConfigHolder) o;
        return Objects.equals(discoveryStrategyConfigs, that.discoveryStrategyConfigs) && Objects.equals(discoveryServiceProvider,
                that.discoveryServiceProvider) && Objects.equals(nodeFilter, that.nodeFilter) && Objects.equals(nodeFilterClass,
                that.nodeFilterClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(discoveryStrategyConfigs, discoveryServiceProvider, nodeFilter, nodeFilterClass);
    }
}
