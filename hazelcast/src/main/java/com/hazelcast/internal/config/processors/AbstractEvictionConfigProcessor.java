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
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.spi.eviction.EvictionPolicyComparator;

import org.w3c.dom.Node;

import static com.hazelcast.internal.config.DomConfigHelper.getIntegerValue;
import static com.hazelcast.internal.config.DomConfigHelper.getTextContent;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static com.hazelcast.internal.util.StringUtil.upperCaseInternal;

public abstract class AbstractEvictionConfigProcessor implements Processor<EvictionConfig> {
    private final Node node;
    private final boolean domLevel3;

    AbstractEvictionConfigProcessor(Node node, boolean domLevel3) {
        this.node = node;
        this.domLevel3 = domLevel3;
    }

    abstract EvictionConfig evictionConfigWithDefaults();

    abstract void validate(EvictionConfig evictionConfig);

    abstract int sizeDefault();

    abstract EvictionPolicy defaultEvictionPolicy();

    @Override
    public EvictionConfig process() {
        EvictionConfig evictionConfig = evictionConfigWithDefaults();

        Node sizeNode = node.getAttributes().getNamedItem("size");
        Node maxSizePolicyNode = node.getAttributes().getNamedItem("max-size-policy");
        Node evictionPolicyNode = node.getAttributes().getNamedItem("eviction-policy");
        Node comparatorClassNameNode = node.getAttributes().getNamedItem("comparator-class-name");

        if (sizeNode != null) {
            evictionConfig.setSize(getIntegerValue("size", getTextContent(sizeNode, domLevel3)));
            if (evictionConfig.getSize() == 0) {
                evictionConfig.setSize(sizeDefault());
            }
        }
        if (maxSizePolicyNode != null) {
            evictionConfig.setMaxSizePolicy(
                MaxSizePolicy.valueOf(upperCaseInternal(getTextContent(maxSizePolicyNode, domLevel3))));
        }
        if (evictionPolicyNode != null) {
            evictionConfig.setEvictionPolicy(
                EvictionPolicy.valueOf(upperCaseInternal(getTextContent(evictionPolicyNode, domLevel3))));
        }
        if (comparatorClassNameNode != null) {
            evictionConfig.setComparatorClassName(getTextContent(comparatorClassNameNode, domLevel3));
        }

        try {
            validate(evictionConfig);
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigurationException(e.getMessage());
        }
        return evictionConfig;
    }

    void assertOnlyComparatorClassOrComparatorConfigured(String comparatorClassName, EvictionPolicyComparator comparator) {
        if (comparatorClassName != null && comparator != null) {
            throw new InvalidConfigurationException("Only one of the `comparator class name` and `comparator`"
                                                        + " can be configured in the eviction configuration!");
        }
    }

    void assertOnlyEvictionPolicyOrComparatorClassNameOrComparatorConfigured(EvictionPolicy evictionPolicy,
                                                                             String comparatorClassName,
                                                                             EvictionPolicyComparator comparator) {
        if (evictionPolicy != defaultEvictionPolicy()) {
            if (!isNullOrEmpty(comparatorClassName)) {
                throw new InvalidConfigurationException(
                    "Only one of the `eviction policy` and `comparator class name` can be configured!");
            }
            if (comparator != null) {
                throw new InvalidConfigurationException(
                    "Only one of the `eviction policy` and `comparator` can be configured!");
            }
        }
    }
}
