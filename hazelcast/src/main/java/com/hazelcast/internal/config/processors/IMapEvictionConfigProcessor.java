/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.config.processors;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.internal.config.ConfigValidator;
import com.hazelcast.spi.eviction.EvictionPolicyComparator;

import org.w3c.dom.Node;

import java.util.EnumSet;

import static java.lang.String.format;

class IMapEvictionConfigProcessor extends AbstractEvictionConfigProcessor {
    IMapEvictionConfigProcessor(Node node, boolean domLevel3) {
        super(node, domLevel3);
    }

    @Override
    EvictionConfig evictionConfigWithDefaults() {
        return new EvictionConfig()
            .setEvictionPolicy(MapConfig.DEFAULT_EVICTION_POLICY)
            .setMaxSizePolicy(MapConfig.DEFAULT_MAX_SIZE_POLICY)
            .setSize(MapConfig.DEFAULT_MAX_SIZE);
    }

    @Override
    int sizeDefault() {
        return MapConfig.DEFAULT_MAX_SIZE;
    }

    @Override
    EvictionPolicy defaultEvictionPolicy() {
        return MapConfig.DEFAULT_EVICTION_POLICY;
    }

    @Override
    void validate(EvictionConfig evictionConfig) {
        EvictionPolicy evictionPolicy = evictionConfig.getEvictionPolicy();
        String comparatorClassName = evictionConfig.getComparatorClassName();
        EvictionPolicyComparator comparator = evictionConfig.getComparator();

        assertOnlyComparatorClassOrComparatorConfigured(comparatorClassName, comparator);
        assertOnlyEvictionPolicyOrComparatorClassNameOrComparatorConfigured(evictionPolicy, comparatorClassName, comparator);

        assertSupportedMaxSizePolicyIsSet(evictionConfig.getMaxSizePolicy());
    }

    private void assertSupportedMaxSizePolicyIsSet(MaxSizePolicy maxSizePolicy) {
        if (!ConfigValidator.MAP_SUPPORTED_ON_HEAP_MAX_SIZE_POLICIES.contains(maxSizePolicy)
            && !ConfigValidator.MAP_SUPPORTED_NATIVE_MAX_SIZE_POLICIES.contains(maxSizePolicy)) {

            EnumSet<MaxSizePolicy> allMaxSizePolicies = EnumSet.copyOf(ConfigValidator.MAP_SUPPORTED_ON_HEAP_MAX_SIZE_POLICIES);
            allMaxSizePolicies.addAll(ConfigValidator.MAP_SUPPORTED_NATIVE_MAX_SIZE_POLICIES);

            String msg = format("IMap eviction config doesn't support max size policy `%s`. "
                                    + "Please select a valid one: %s.", maxSizePolicy, allMaxSizePolicies);

            throw new InvalidConfigurationException(msg);
        }
    }
}
