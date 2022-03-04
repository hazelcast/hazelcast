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

package com.hazelcast.cardinality.impl.hyperloglog.impl;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public interface HyperLogLogEncoder extends IdentifiedDataSerializable {

    /**
     * Computes a new estimate for the current status of the registers.
     *
     * @return a cardinality estimation.
     */
    long estimate();

    /**
     * Aggregates the hash value in the HyperLogLog registers, and returns a hint if the
     * operation might have affected the cardinality, it is just a hint, and it relies to
     * the respective implementation.
     *
     * @param hash the value to aggregate
     * @return boolean hint, if the latest operation might have affected the cardinality.
     */
    boolean add(long hash);

    /**
     * Returns the size in memory occupied (in bytes) for this implementation of HyperLogLog.
     *
     * @return the size in memory.
     */
    int getMemoryFootprint();


    /**
     * Returns the encoding type of this instance; see: {@link HyperLogLogEncoding}
     *
     * @return {@link HyperLogLogEncoding}
     */
    HyperLogLogEncoding getEncodingType();

    /**
     * Merge the two HyperLogLog structures in one. Estimations from both are taken into consideration
     * and the unified estimate should be similar to the distinct union set of the two.
     *
     * @param encoder The second HLL to be merged into this one
     * @return {@link HyperLogLogEncoder} the union
     */
    HyperLogLogEncoder merge(HyperLogLogEncoder encoder);
}
