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

package com.hazelcast.map.impl;

import com.hazelcast.config.InMemoryFormat;

import static com.hazelcast.config.InMemoryFormat.BINARY;

/**
 * Static factory methods for various size estimators.
 */
public final class SizeEstimatorFactory {

    /**
     * Returns zero for all estimations.
     */
    private static final SizeEstimator ZERO_SIZE_ESTIMATOR = new ZeroSizeEstimator();

    private SizeEstimatorFactory() {
    }

    public static SizeEstimator createMapSizeEstimator(InMemoryFormat inMemoryFormat) {
        if (BINARY.equals(inMemoryFormat)) {
            return new BinaryMapSizeEstimator();
        }
        return ZERO_SIZE_ESTIMATOR;
    }

    private static class ZeroSizeEstimator implements SizeEstimator {

        @Override
        public long getSize() {
            return 0L;
        }

        @Override
        public void add(long size) {
        }

        @Override
        public long calculateSize(Object object) {
            return 0L;
        }

        @Override
        public void reset() {
        }
    }
}
