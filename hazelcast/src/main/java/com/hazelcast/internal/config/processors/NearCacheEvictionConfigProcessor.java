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
import com.hazelcast.spi.eviction.EvictionPolicyComparator;

import org.w3c.dom.Node;

class NearCacheEvictionConfigProcessor extends AbstractEvictionConfigProcessor {
    NearCacheEvictionConfigProcessor(Node node, boolean domLevel3) {
        super(node, domLevel3);
    }

    @Override
    EvictionConfig evictionConfigWithDefaults() {
        return new EvictionConfig();
    }

    @Override
    void validate(EvictionConfig evictionConfig) {
        EvictionPolicy evictionPolicy = evictionConfig.getEvictionPolicy();
        String comparatorClassName = evictionConfig.getComparatorClassName();
        EvictionPolicyComparator comparator = evictionConfig.getComparator();

        assertOnlyComparatorClassOrComparatorConfigured(comparatorClassName, comparator);
        assertOnlyEvictionPolicyOrComparatorClassNameOrComparatorConfigured(
            evictionPolicy,
            comparatorClassName,
            comparator
        );
    }

    @Override
    int sizeDefault() {
        return 0;
    }

    @Override
    EvictionPolicy defaultEvictionPolicy() {
        return EvictionConfig.DEFAULT_EVICTION_POLICY;
    }
}
