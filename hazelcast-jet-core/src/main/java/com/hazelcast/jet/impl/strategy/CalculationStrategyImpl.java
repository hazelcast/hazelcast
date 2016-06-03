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
import com.hazelcast.jet.data.tuple.JetTuple;
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
        Object partitionKey;

        if (object instanceof JetTuple) {
            partitionKey = ((JetTuple) object).getKeyData(this, this.containerDescriptor.getNodeEngine());
        } else {
            partitionKey = this.partitioningStrategy.getPartitionKey(object);
        }

        return this.hashingStrategy.hash(object, partitionKey, this.containerDescriptor);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CalculationStrategyImpl that = (CalculationStrategyImpl) o;

        if (!hashingStrategyClass.equals(that.hashingStrategyClass)) {
            return false;
        }

        return partitioningStrategyClass.equals(that.partitioningStrategyClass);
    }

    @Override
    public int hashCode() {
        int result = hashingStrategyClass.hashCode();
        result = 31 * result + partitioningStrategyClass.hashCode();
        return result;
    }
}
