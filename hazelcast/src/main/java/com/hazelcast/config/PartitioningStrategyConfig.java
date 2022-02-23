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

package com.hazelcast.config;

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.PartitioningStrategy;

import java.io.IOException;

/**
 * Contains the configuration for partitioning strategy.
 */
public class PartitioningStrategyConfig implements IdentifiedDataSerializable {

    private String partitioningStrategyClass;

    private PartitioningStrategy partitioningStrategy;

    public PartitioningStrategyConfig() {
    }

    public PartitioningStrategyConfig(PartitioningStrategyConfig config) {
        partitioningStrategyClass = config.getPartitioningStrategyClass();
        partitioningStrategy = config.getPartitioningStrategy();
    }

    public PartitioningStrategyConfig(String partitioningStrategyClass) {
        this.partitioningStrategyClass = partitioningStrategyClass;
    }

    public PartitioningStrategyConfig(PartitioningStrategy partitioningStrategy) {
        this.partitioningStrategy = partitioningStrategy;
    }

    public String getPartitioningStrategyClass() {
        return partitioningStrategyClass;
    }

    public PartitioningStrategyConfig setPartitioningStrategyClass(String partitionStrategyClass) {
        this.partitioningStrategyClass = partitionStrategyClass;
        return this;
    }

    public PartitioningStrategy getPartitioningStrategy() {
        return partitioningStrategy;
    }

    public PartitioningStrategyConfig setPartitioningStrategy(PartitioningStrategy partitionStrategy) {
        this.partitioningStrategy = partitionStrategy;
        return this;
    }

    @Override
    public String toString() {
        return "PartitioningStrategyConfig{"
                + "partitioningStrategyClass='" + partitioningStrategyClass + '\''
                + ", partitioningStrategy=" + partitioningStrategy
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.PARTITION_STRATEGY_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(partitioningStrategyClass);
        out.writeObject(partitioningStrategy);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        partitioningStrategyClass = in.readString();
        partitioningStrategy = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PartitioningStrategyConfig that = (PartitioningStrategyConfig) o;

        if (partitioningStrategyClass != null
                ? !partitioningStrategyClass.equals(that.partitioningStrategyClass)
                : that.partitioningStrategyClass != null) {
            return false;
        }
        return partitioningStrategy != null ? partitioningStrategy.equals(that.partitioningStrategy)
                : that.partitioningStrategy == null;
    }

    @Override
    public int hashCode() {
        int result = partitioningStrategyClass != null ? partitioningStrategyClass.hashCode() : 0;
        result = 31 * result + (partitioningStrategy != null ? partitioningStrategy.hashCode() : 0);
        return result;
    }
}
