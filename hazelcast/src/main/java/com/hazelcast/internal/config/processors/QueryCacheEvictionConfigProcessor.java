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
import com.hazelcast.internal.config.ConfigValidator;
import com.hazelcast.spi.eviction.EvictionPolicyComparator;

import org.w3c.dom.Node;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static java.lang.String.format;

class QueryCacheEvictionConfigProcessor extends AbstractEvictionConfigProcessor {
    QueryCacheEvictionConfigProcessor(Node node, boolean domLevel3) {
        super(node, domLevel3);
    }

    @Override
    EvictionConfig evictionConfigWithDefaults() {
        return new EvictionConfig();
    }

    @Override
    int sizeDefault() {
        return 0;
    }

    @Override
    EvictionPolicy defaultEvictionPolicy() {
        return EvictionConfig.DEFAULT_EVICTION_POLICY;
    }

    @Override
    void validate(EvictionConfig evictionConfig) {
        EvictionPolicy evictionPolicy = evictionConfig.getEvictionPolicy();
        String comparatorClassName = evictionConfig.getComparatorClassName();
        EvictionPolicyComparator comparator = evictionConfig.getComparator();

        assertOnlyComparatorClassOrComparatorConfigured(comparatorClassName, comparator);

        if (!ConfigValidator.COMMONLY_SUPPORTED_EVICTION_POLICIES.contains(evictionPolicy)) {
            assertSupportedEvictionPolicyOrComparatorIsSet(evictionPolicy, comparatorClassName, comparator);
        } else {
            assertOnlyEvictionPolicyOrComparatorClassNameOrComparatorConfigured(
                evictionPolicy,
                comparatorClassName,
                comparator
            );
        }
    }

    private void assertSupportedEvictionPolicyOrComparatorIsSet(EvictionPolicy evictionPolicy,
                                                                String comparatorClassName,
                                                                EvictionPolicyComparator comparator) {
        if (isNullOrEmpty(comparatorClassName) && comparator == null) {
            String msg = format(
                "Eviction policy `%s` is not supported. Either you can provide a custom one or "
                    + "you can use a supported one: %s.",
                evictionPolicy,
                ConfigValidator.COMMONLY_SUPPORTED_EVICTION_POLICIES
            );

            throw new InvalidConfigurationException(msg);
        }
    }
}
