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

package com.hazelcast.jet.impl.strategy;


import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.container.ContainerDescriptor;
import com.hazelcast.jet.data.tuple.JetTuple2;
import com.hazelcast.jet.strategy.CalculationStrategy;
import com.hazelcast.jet.strategy.HashingStrategy;

/**
 * Default calculation strategy implementation;
 * <p>
 * Calculation strategy joins 2 abstractions:
 * <pre>
 *      -   HashingStrategy
 *      -   PartitioningStrategy
 * </pre>
 */
public class CalculationStrategyImpl implements CalculationStrategy {
    private final Class hashingStrategyClass;

    private final HashingStrategy hashingStrategy;

    private final Class partitioningStrategyClass;

    private final ContainerDescriptor containerDescriptor;

    private final PartitioningStrategy partitioningStrategy;

    public CalculationStrategyImpl(
            HashingStrategy hashingStrategy,
            PartitioningStrategy partitioningStrategy,
            ContainerDescriptor containerDescriptor
    ) {
        this.hashingStrategy = hashingStrategy;
        this.containerDescriptor = containerDescriptor;
        this.partitioningStrategy = partitioningStrategy;

        this.hashingStrategyClass = this.hashingStrategy.getClass();
        this.partitioningStrategyClass = this.partitioningStrategy.getClass();
    }

    @Override
    public PartitioningStrategy getPartitioningStrategy() {
        return this.partitioningStrategy;
    }

    @Override
    public HashingStrategy getHashingStrategy() {
        return this.hashingStrategy;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int hash(Object object) {
        final Object partitionKey = object instanceof JetTuple2
                ? ((JetTuple2) object).getComponentData(0, this, containerDescriptor.getNodeEngine())
                : partitioningStrategy.getPartitionKey(object);
        return hashingStrategy.hash(object, partitionKey, containerDescriptor);
    }

    @Override
    @SuppressWarnings("checkstyle:innerassignment")
    public boolean equals(Object o) {
        final CalculationStrategyImpl that;
        return this == o
                || o != null
                   && getClass() == o.getClass()
                   && this.hashingStrategyClass.equals((that = (CalculationStrategyImpl) o).hashingStrategyClass)
                   && this.partitioningStrategyClass.equals(that.partitioningStrategyClass);
    }

    @Override
    public int hashCode() {
        int result = hashingStrategyClass.hashCode();
        result = 31 * result + partitioningStrategyClass.hashCode();
        return result;
    }
}
