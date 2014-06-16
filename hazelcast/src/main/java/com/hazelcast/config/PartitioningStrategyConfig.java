/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.core.PartitioningStrategy;

public class PartitioningStrategyConfig {

    private String partitioningStrategyClass;

    private PartitioningStrategy partitionStrategy;

    private PartitioningStrategyConfigReadOnly readOnly;

    public PartitioningStrategyConfig() {
    }

    public PartitioningStrategyConfig(PartitioningStrategyConfig config) {
        partitioningStrategyClass = config.getPartitioningStrategyClass();
        partitionStrategy = config.getPartitioningStrategy();
    }

    public PartitioningStrategyConfig(String partitioningStrategyClass) {
        this.partitioningStrategyClass = partitioningStrategyClass;
    }

    public PartitioningStrategyConfig(PartitioningStrategy partitionStrategy) {
        this.partitionStrategy = partitionStrategy;
    }

    public PartitioningStrategyConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new PartitioningStrategyConfigReadOnly(this);
        }
        return readOnly;
    }

    public String getPartitioningStrategyClass() {
        return partitioningStrategyClass;
    }

    public PartitioningStrategyConfig setPartitioningStrategyClass(String partitionStrategyClass) {
        this.partitioningStrategyClass = partitionStrategyClass;
        return this;
    }

    public PartitioningStrategy getPartitioningStrategy() {
        return partitionStrategy;
    }

    public PartitioningStrategyConfig setPartitionStrategy(PartitioningStrategy partitionStrategy) {
        this.partitionStrategy = partitionStrategy;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PartitioningStrategyConfig{");
        sb.append("partitioningStrategyClass='").append(partitioningStrategyClass).append('\'');
        sb.append(", partitionStrategy=").append(partitionStrategy);
        sb.append('}');
        return sb.toString();
    }
}
