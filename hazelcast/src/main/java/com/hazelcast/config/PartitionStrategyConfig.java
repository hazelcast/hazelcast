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

public class PartitionStrategyConfig {

    private String partitionStrategyClass;

    private PartitioningStrategy partitionStrategy;

    public PartitionStrategyConfig() {
    }

    public PartitionStrategyConfig(String partitionStrategyClass) {
        this.partitionStrategyClass = partitionStrategyClass;
    }

    public PartitionStrategyConfig(PartitioningStrategy partitionStrategy) {
        this.partitionStrategy = partitionStrategy;
    }

    public String getPartitionStrategyClass() {
        return partitionStrategyClass;
    }

    public PartitionStrategyConfig setPartitionStrategyClass(String partitionStrategyClass) {
        this.partitionStrategyClass = partitionStrategyClass;
        return this;
    }

    public PartitioningStrategy getPartitionStrategy() {
        return partitionStrategy;
    }

    public PartitionStrategyConfig setPartitionStrategy(PartitioningStrategy partitionStrategy) {
        this.partitionStrategy = partitionStrategy;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PartitionStrategyConfig{");
        sb.append("partitionStrategyClass='").append(partitionStrategyClass).append('\'');
        sb.append(", partitionStrategy=").append(partitionStrategy);
        sb.append('}');
        return sb.toString();
    }
}
