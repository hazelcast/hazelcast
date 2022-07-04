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

import com.hazelcast.config.InMemoryFormat;

import static com.hazelcast.config.InMemoryFormat.BINARY;

/**
 * Static factory methods for various entry cost estimators.
 */
public final class OwnedEntryCostEstimatorFactory {

    /**
     * Returns zero for all estimations.
     */
    public static final EntryCostEstimator ZERO_SIZE_ESTIMATOR = new ZeroEntryCostEstimator();

    private OwnedEntryCostEstimatorFactory() {
    }

    public static <K, V> EntryCostEstimator<K, V> createMapSizeEstimator(InMemoryFormat inMemoryFormat) {
        if (BINARY.equals(inMemoryFormat)) {
            return (EntryCostEstimator<K, V>) new BinaryMapEntryCostEstimator();
        }
        return ZERO_SIZE_ESTIMATOR;
    }

    private static class ZeroEntryCostEstimator
            implements EntryCostEstimator {

        @Override
        public long getEstimate() {
            return 0;
        }

        @Override
        public void adjustEstimateBy(long adjustment) {
        }

        @Override
        public long calculateValueCost(Object value) {
            return 0;
        }

        @Override
        public long calculateEntryCost(Object key, Object value) {
            return 0;
        }

        @Override
        public void reset() {
        }
    }
}
