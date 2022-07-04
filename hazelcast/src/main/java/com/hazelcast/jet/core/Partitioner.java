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

package com.hazelcast.jet.core;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * Encapsulates the logic associated with a {@link DAG} edge that decides
 * on the partition ID of an item traveling over it. The partition ID
 * determines which cluster member and which instance of {@link Processor} on
 * that member an item will be forwarded to.
 * <p>
 * Jet's partitioning piggybacks on Hazelcast partitioning. Standard Hazelcast
 * protocols are used to distribute partition ownership over the members of the
 * cluster. However, if a DAG edge is configured as non-distributed, then on each
 * member there will be some destination processor responsible for any given
 * partition.
 *
 * @param <T> type of item the partitioner accepts
 *
 * @since Jet 3.0
 */
@FunctionalInterface
public interface Partitioner<T> extends Serializable {

    /**
     * Partitioner which calls {@link Object#hashCode()} and coerces it with the
     * modulo operation into the allowed range of partition IDs. The primary
     * reason to prefer this over the default is performance and it's a safe
     * choice on local edges.
     * <p>
     * <strong>WARNING:</strong> this is a dangerous strategy to use on
     * distributed edges. Care must be taken to ensure that the produced
     * hashcode remains stable across serialization-deserialization cycles as
     * well as across all JVM processes. Consider a {@code hashCode()} method
     * that is correct with respect to its contract, but not with respect to
     * the stricter contract given above. Take the following scenario:
     * <ol><li>
     *     there are two Jet cluster members;
     * </li><li>
     *     there is a DAG vertex;
     * </li><li>
     *     on each member there is a processor for this vertex;
     * </li><li>
     *     each processor emits an item;
     * </li><li>
     *     these two items have equal partitioning keys;
     * </li><li>
     *     nevertheless, on each member they get a different hashcode;
     * </li><li>
     *     they are routed to different processors, thus failing on the promise
     *     that all items with the same partition key go to the same processor.
     * </li></ol>
     */
    Partitioner<Object> HASH_CODE = (item, partitionCount) -> Math.abs(item.hashCode() % partitionCount);

    /**
     * Callback that injects the Hazelcast's default partitioning strategy into
     * this partitioner so it can be consulted by the
     * {@link #getPartition(Object, int)} method.
     * <p>
     * The creation of instances of the {@code Partitioner} type is done in
     * user's code, but the Hazelcast partitioning strategy only becomes
     * available after the partitioner is deserialized on each target member.
     * This method solves the lifecycle mismatch.
     */
    default void init(@Nonnull DefaultPartitionStrategy strat) {
    }

    /**
     * Returns the partition ID of the given item.
     *
     * @param partitionCount the total number of partitions in use by the underlying Hazelcast instance
     */
    int getPartition(@Nonnull T item, int partitionCount);

    /**
     * Returns a partitioner which applies the default Hazelcast partitioning.
     * It will serialize the item using Hazelcast Serialization, then apply
     * Hazelcast's {@code MurmurHash}-based algorithm to retrieve the partition
     * ID. This is quite a bit of work, but has stable results across all JVM
     * processes, making it a safe default.
     */
    static Partitioner<Object> defaultPartitioner() {
        return new Default();
    }

    /**
     * Partitioner which applies the default Hazelcast partitioning strategy.
     * Instances should be retrieved from {@link #defaultPartitioner()}.
     */
    final class Default implements Partitioner<Object> {

        private static final long serialVersionUID = 1L;

        transient DefaultPartitionStrategy defaultPartitioning;

        Default() {
        }

        @Override
        public void init(@Nonnull DefaultPartitionStrategy defaultPartitioning) {
            this.defaultPartitioning = defaultPartitioning;
        }

        @Override
        public int getPartition(@Nonnull Object item, int partitionCount) {
            return defaultPartitioning.getPartition(item);
        }
    }
}
