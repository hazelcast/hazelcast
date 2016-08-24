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

package com.hazelcast.jet.strategy;


import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.container.ContainerDescriptor;

/**
 * Default calculation strategy implementation;
 * <p/>
 * Calculation strategy joins 2 abstractions:
 * <pre>
 *      -   HashingStrategy
 *      -   PartitioningStrategy
 * </pre>
 */
public class CalculationStrategy {

    private final HashingStrategy hashingStrategy;
    private final ContainerDescriptor containerDescriptor;
    private final PartitioningStrategy partitioningStrategy;

    /**
     * Creates a new calculation strategy
     */
    public CalculationStrategy(
            HashingStrategy hashingStrategy,
            PartitioningStrategy partitioningStrategy,
            ContainerDescriptor containerDescriptor
    ) {
        this.hashingStrategy = hashingStrategy;
        this.containerDescriptor = containerDescriptor;
        this.partitioningStrategy = partitioningStrategy;
    }

    /**
     * @return corresponding partitioningStrategy
     */
    public PartitioningStrategy getPartitioningStrategy() {
        return this.partitioningStrategy;
    }

    /**
     * @return corresponding hashingStrategy
     */
    public HashingStrategy getHashingStrategy() {
        return this.hashingStrategy;
    }

    /**
     * Calculates hash of the corresponding object
     *
     * @param object object for hash calculation
     * @return corresponding hash
     */
    @SuppressWarnings("unchecked")
    public int hash(Object object) {
        final Object partitionKey = partitioningStrategy.getPartitionKey(object);
        return hashingStrategy.hash(object, partitionKey, containerDescriptor);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CalculationStrategy that = (CalculationStrategy) o;

        if (!hashingStrategy.equals(that.hashingStrategy)) {
            return false;
        }
        if (!containerDescriptor.equals(that.containerDescriptor)) {
            return false;
        }
        return partitioningStrategy.equals(that.partitioningStrategy);

    }

    @Override
    public int hashCode() {
        int result = hashingStrategy.hashCode();
        result = 31 * result + containerDescriptor.hashCode();
        result = 31 * result + partitioningStrategy.hashCode();
        return result;
    }
}
