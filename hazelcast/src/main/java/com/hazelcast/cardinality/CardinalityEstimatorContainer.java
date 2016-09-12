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

package com.hazelcast.cardinality;

import com.hazelcast.cardinality.hyperloglog.IHyperLogLog;
import com.hazelcast.cardinality.hyperloglog.impl.HyperLogLogEncType;

public class CardinalityEstimatorContainer {

    private static final int DEFAULT_HLL_PRECISION = 14;

    private final IHyperLogLog hll;

    public CardinalityEstimatorContainer() {
        hll = HyperLogLogEncType.COMPO.build(DEFAULT_HLL_PRECISION);
    }


    public boolean aggregate(long hash) {
        return hll.aggregate(hash);
    }

    public boolean aggregateAll(long[] hashes) {
        return hll.aggregateAll(hashes);
    }

    public long estimate() {
        return hll.estimate();
    }

}
