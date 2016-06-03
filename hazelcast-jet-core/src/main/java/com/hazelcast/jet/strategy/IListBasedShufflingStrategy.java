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

import com.hazelcast.jet.container.ContainerDescriptor;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.NodeEngine;

/**
 * Shuffling strategy based on IList internal logic;
 * <p>
 * All data of Hazelcast list are stored on exactly one node;
 * <p>
 * Using this shuffling strategy all JET-data will be sent to this IList's node;
 */
public class IListBasedShufflingStrategy implements ShufflingStrategy {
    private final String listName;

    public IListBasedShufflingStrategy(String listName) {
        this.listName = listName;
    }

    @Override
    public Address[] getShufflingAddress(ContainerDescriptor containerDescriptor) {
        NodeEngine nodeEngine = containerDescriptor.getNodeEngine();

        int partitionId = nodeEngine.getPartitionService().getPartitionId(
                nodeEngine.getSerializationService().toData(
                        this.listName,
                        StringPartitioningStrategy.INSTANCE
                )
        );

        return new Address[]{nodeEngine.getPartitionService().getPartitionOwner(partitionId)};
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IListBasedShufflingStrategy that = (IListBasedShufflingStrategy) o;

        return !(listName != null ? !listName.equals(that.listName) : that.listName != null);

    }

    @Override
    public int hashCode() {
        return listName != null ? listName.hashCode() : 0;
    }
}
