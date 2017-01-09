/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.jet.stream.Distributed;

import java.io.Serializable;

/**
 * Encapsulates the logic associated with a {@link DAG DAG} edge that decides
 * on the partition ID of an item traveling over it. The partition ID
 * determines which cluster member and which instance of {@link Processor} on
 * that member an item will be forwarded to.
 * <p>
 * Jet's partitioning piggybacks on Hazelcast partitioning. Standard Hazelcast
 * protocols are used to distribute partition ownership over the members of the
 * cluster. However, if a DAG edge is configured as non-distributed, then on each
 * member there will be some destination processor responsible for any given partition.
 */
@FunctionalInterface
public interface Partitioner extends Serializable {

    /**
     * Callback that injects the Hazelcast's default partitioning strategy into
     * this partitioner so it can be consulted by the {@link #getPartition(Object, int)} method.
     * <p>
     * The creation of instances of the {@code Partitioner} type is done in user's code,
     * but the Hazelcast partitioning strategy only becomes available after the partitioner
     * is deserialized on each target member. This method solves the lifecycle mismatch.
     */
    default void init(DefaultPartitionStrategy lookup) {
    }

    /**
     * @param item the item for which to determine the partition ID
     * @param partitionCount the total number of partitions as configured for the underlying Hazelcast instance
     * @return the partition ID of the given item
     */
    int getPartition(Object item, int partitionCount);

    /**
     * Takes a hashing function that maps the object to an {@code int} and
     * returns a partitioner that transforms the unconstrained {@code int}
     * value to a legal partition ID.
     */
    static Partitioner fromInt(Distributed.ToIntFunction<Object> hasher) {
        return (item, partitionCount) -> Math.abs(hasher.applyAsInt(item) % partitionCount);
    }
}
