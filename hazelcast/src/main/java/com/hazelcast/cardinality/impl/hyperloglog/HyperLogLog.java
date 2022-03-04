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

package com.hazelcast.cardinality.impl.hyperloglog;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * HyperLogLog is a redundant and highly available distributed data-structure used for cardinality estimation
 * purposes on unique items in significantly sized data cultures. HyperLogLog uses P^2 byte registers for storage
 * and computation.
 *
 * HyperLogLog is an implementation of the two famous papers
 * <ul>
 *     <li>http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf</li>
 *     <li>http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf</li>
 * </ul>
 *
 */
public interface HyperLogLog extends IdentifiedDataSerializable {

    /**
     * Computes a new estimate for the current status of the registers.
     * If it was previously estimated and never invalidated, then a cached version is used.
     *
     * @return previously cached estimation or a newly computed one.
     */
    long estimate();

    /**
     * Aggregates the hash in the HyperLogLog registers.
     *
     * @param hash the value to aggregate
     */
    void add(long hash);

    /**
     * Batch aggregation of hash values in the HyperLogLog registers.
     *
     * @param hashes the hash values array to aggregate
     */
    void addAll(long[] hashes);

    /**
     * Merge the two HyperLogLog structures in one. Estimations from both are taken into consideration
     * and the unified estimate should be similar to the distinct union set of the two.
     *
     * @param other The second HLL to be merged into this one
     */
    void merge(HyperLogLog other);
}
