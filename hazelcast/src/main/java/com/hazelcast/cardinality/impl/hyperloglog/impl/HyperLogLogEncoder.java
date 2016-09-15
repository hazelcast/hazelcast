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

package com.hazelcast.cardinality.impl.hyperloglog.impl;

import com.hazelcast.nio.serialization.DataSerializable;

public interface HyperLogLogEncoder extends DataSerializable {

    /**
     * Computes a new estimate for the current status of the registers.
     * If it was previously estimated and never invalidated, then the cached version is used.
     *
     * @return Returns the previous cached estimation or the newly computed one.
     */
    long estimate();

    /**
     * Aggregates the hashcode in the HyperLogLog registers.
     *
     * @param hash the value to aggregate
     * @return boolean flag when the underlying registers got modified, meaning a new estimate can be computed.
     */
    boolean aggregate(long hash);

    /**
     * Returns the size in memory occupied (in bits) for this implementation of HyperLogLog.
     *
     * @return the size in memory.
     */
    int getMemoryFootprint();


    /**
     * Returns the encoding type of this instance; see: {@link HyperLogLogEncoding}
     * @return {@link HyperLogLogEncoding}
     */
    HyperLogLogEncoding getEncodingType();

}
