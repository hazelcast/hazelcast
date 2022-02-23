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

package com.hazelcast.map.impl;

/**
 * Owned entry cost estimator general contract.
 *
 * @param <K> the type of key which's size going to be estimated.
 * @param <V> the type of key which's size going to be estimated.
 */
public interface EntryCostEstimator<K, V> {

    /**
     * Returns the memory cost estimation so far
     * @return the memory cost estimation so far
     */
    long getEstimate();

    /**
     * Adjust the memory cost estimation by the given adjustment.
     * The adjustmenet can be any negative or positive number.
     *
     * @param adjustment The delta by which the estimation will be adjusted
     */
    void adjustEstimateBy(long adjustment);

    /**
     * Calculate the entry's value cost in memory
     * @param value The entry's value
     * @return The cost of this value in memory.
     */
    long calculateValueCost(V value);

    /**
     * Calculate the entry's cost in memory
     * @param key The key of the entry
     * @param value The value of the entry
     * @return The memory cost of this key/value pair, plus the entry structure itself.
     */
    long calculateEntryCost(K key, V value);

    /**
     * Reset the current estimation to zero.
     */
    void reset();
}
