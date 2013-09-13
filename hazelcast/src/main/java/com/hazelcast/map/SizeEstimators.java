/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

/**
 * Created with IntelliJ IDEA.
 * User: ahmet
 * Date: 08.09.2013
 * Time: 14:04
 */
public final class SizeEstimators {

    private SizeEstimators(){  }

    public static SizeEstimator createMapSizeEstimator( boolean statisticsEnabled )
    {
        final SizeEstimator mapSizeEstimator = statisticsEnabled ? new MapSizeEstimator() : EMPTY_SIZE_ESTIMATOR;

        return mapSizeEstimator;
    }

    static final SizeEstimator EMPTY_SIZE_ESTIMATOR = new SizeEstimator(){

        @Override
        public long getSize() {
            //not implemented
            return 0;
        }

        @Override
        public void add(long size) {
            //not implemented
        }

        @Override
        public <T> long getCost(T record) {
            //not implemented
            return 0;
        }

        @Override
        public void reset() {
            //not implemented
        }
    };
}
