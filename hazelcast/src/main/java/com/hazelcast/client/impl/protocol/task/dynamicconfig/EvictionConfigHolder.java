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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.internal.eviction.EvictionPolicyComparator;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

/**
 * Client protocol adapter for (@link {@link com.hazelcast.config.EvictionConfig}
 */
public class EvictionConfigHolder {

    private final int size;
    private final String maxSizePolicy;
    private final String evictionPolicy;
    private final String comparatorClassName;
    private final Data comparator;

    public EvictionConfigHolder(int size, String maxSizePolicy, String evictionPolicy,
                                String comparatorClassName, Data comparator) {
        this.size = size;
        this.maxSizePolicy = maxSizePolicy;
        this.evictionPolicy = evictionPolicy;
        this.comparatorClassName = comparatorClassName;
        this.comparator = comparator;
    }

    public int getSize() {
        return size;
    }

    public String getMaxSizePolicy() {
        return maxSizePolicy;
    }

    public String getEvictionPolicy() {
        return evictionPolicy;
    }

    public String getComparatorClassName() {
        return comparatorClassName;
    }

    public Data getComparator() {
        return comparator;
    }

    public EvictionConfig asEvictionConfg(SerializationService serializationService) {
        EvictionConfig config = new EvictionConfig(size, EvictionConfig.MaxSizePolicy.valueOf(maxSizePolicy),
                EvictionPolicy.valueOf(evictionPolicy));
        if (comparatorClassName != null) {
            config.setComparatorClassName(comparatorClassName);
        }
        if (comparator != null) {
            EvictionPolicyComparator evictionPolicyComparator = serializationService.toObject(comparator);
            config.setComparator(evictionPolicyComparator);
        }
        return config;
    }

    public static EvictionConfigHolder of(EvictionConfig config, SerializationService serializationService) {
        return new EvictionConfigHolder(config.getSize(), config.getMaximumSizePolicy().name(),
                config.getEvictionPolicy().name(), config.getComparatorClassName(),
                serializationService.toData(config.getComparator()));
    }
}
