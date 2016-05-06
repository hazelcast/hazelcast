/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.eviction.impl;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.util.StringUtil;

public final class EvictionConfigHelper {

    private EvictionConfigHelper() {
    }

    public static void checkEvictionConfig(EvictionConfig evictionConfig) {
        if (evictionConfig == null) {
            throw new IllegalArgumentException("Eviction config cannot be null!");
        }
        checkEvictionConfig(evictionConfig.getEvictionPolicy(),
                            evictionConfig.getComparatorClassName(),
                            evictionConfig.getComparator());
    }

    public static void checkEvictionConfig(EvictionPolicy evictionPolicy,
                                           String comparatorClassName,
                                           Object comparator) {
        if (comparatorClassName != null && comparator != null) {
            throw new IllegalArgumentException(
                    "Only one of the `comparator class name` and `comparator`"
                            + " can be configured in the eviction configuration!");
        }
        if (evictionPolicy == null || evictionPolicy == EvictionPolicy.NONE || evictionPolicy == EvictionPolicy.RANDOM) {
            if (StringUtil.isNullOrEmpty(comparatorClassName) && comparator == null) {
                throw new IllegalArgumentException(
                        "Eviction policy must be set to an eviction policy type rather than `null`, `NONE`, `RANDOM`"
                                + " or custom eviction policy comparator must be specified!");
            }
        } else {
            if (evictionPolicy != EvictionConfig.DEFAULT_EVICTION_POLICY) {
                if (!StringUtil.isNullOrEmpty(comparatorClassName)) {
                    throw new IllegalArgumentException(
                            "Only one of the `eviction policy` and `comparator class name` can be configured!");
                }
                if (comparator != null) {
                    throw new IllegalArgumentException(
                            "Only one of the `eviction policy` and `comparator` can be configured!");
                }
            }
        }
    }

}
