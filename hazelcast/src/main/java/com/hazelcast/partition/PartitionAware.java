/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.partition;

/**
 * PartitionAware means that data will be based in the same member based on the partition key
 * and implementing tasks will be executed on the {@link #getPartitionKey()}'s owner member.
 * <p>
 * This achieves data affinity. Data and execution occurs on the same partition.
 * <p>
 * In Hazelcast, disparate data structures will be stored on the same partition,
 * based on the partition key. For example, if "Steve" was used, then the following would be on one partition.
 * <ul>
 *     <li>a customers IMap with an entry of key "Steve"
 *     <li>an orders IMap using a customer key type implementing PartitionAware with key "Steve"
 *     <li>any queue named "Steve"
 *     <li>any PartitionAware object with partition key "Steve"
 * </ul>
 *
 * If you have a {@link com.hazelcast.core.IExecutorService} which needs to deal with a customer and a customer's
 * orders, you can achieve optimal performance by putting them on the same partition.
 * <p>
 * {@link com.hazelcast.core.DistributedObject} also has a notion of the partition key which is of type String
 * to ensure that the same partition as distributed Objects Strings is used for the partition key.
 *
 * @see com.hazelcast.core.DistributedObject
 * @param <T> key type
 */
@FunctionalInterface
public interface PartitionAware<T> {

    /**
     * The key that will be used by Hazelcast to specify the partition.
     * You should give the same key for objects that you want to be in the same partition.
     * <p>
     * The contract of {@link #getPartitionKey()} method is as follows:
     * <p>
     * Let us define {@code toData(o)} as serialized form of given object obtained by using
     * Hazelcast Serialization configured for the cluster (the exact method used is
     * {@link com.hazelcast.internal.serialization.SerializationService#toData SerializationService.toData}
     * from an internal SPI).
     * <p>
     * Assume {@code PartitionAware a, b} are objects (e.g. IMap keys) and
     * {@code T pk1 = a.getPartitionKey(), pk2 = b.getPartitionKey()} are partition key values.
     * <p>
     * Then {@link #getPartitionKey()} implementation must obey the following contract:
     * <ol>
     *     <li>(mandatory) Deterministic partitioning: if {@code a.equals(b)} then either
     *         {@code toData(a.getPartitionKey()).equals(toData(b.getPartitionKey()))}
     *         or {@code a.getPartitionKey() == null && b.getPartitionKey() == null}</li>
     *     <li>(recommended) Reasonable partitioning: if {@code a.equals(b)} then
     *         {@code a.getPartitionKey().equals(b.getPartitionKey())}</li>
     *     <li>(recommended) Reasonable partitioning key serialization (if custom
     *         serialization is used): if {@code pk1.equals(pk2)} then
     *         {@code toData(pk1).equals(toData(pk2))}</li>
     *     <li>The above stated conditions must hold when the {@link #getPartitionKey()}
     *         is invoked any number of times on any member or client, regardless of time
     *         (note that some partitioned data structures support persistence) and if the JVM
     *         is restarted, regardless of client/member version and JVM version (across all JVMs
     *         ever used in given deployment), timezone, locale and similar differences in the
     *         environment.</li>
     * </ol>
     * Adhering to the contract guarantees stable partitioning of the data and ability to find
     * appropriate member and partition who owns given object during querying and modifications.
     * <p>
     * Notes:
     * <ul>
     * <li>{@link #getPartitionKey()} contract is similar to {@link Object#hashCode() hashCode()}
     *     but with stricter long-term requirements.</li>
     * <li>Partition key is not compared directly, only serialized form is compared or used to
     *     calculate partition id.</li>
     * <li>For unequal objects {@link #getPartitionKey()} may return the same or different values
     *     according to specific partitioning use case needs.</li>
     * </ul>
     *
     * @return the key that specifies the partition. Returning {@code null} or {@code this} will
     *         cause the result as if the object did not implement {@link PartitionAware}. If the
     *         returned key itself implements {@link PartitionAware}, this fact will be ignored
     *         and the key will be treated as a plain object.
     */
    T getPartitionKey();

}
