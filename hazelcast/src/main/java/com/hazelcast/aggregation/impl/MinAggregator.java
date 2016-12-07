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

package com.hazelcast.aggregation.impl;

import com.hazelcast.aggregation.Aggregator;

public class MinAggregator<I, R extends Comparable> extends AbstractAggregator<I, R> {

    private R min;

    public MinAggregator() {
        super();
    }

    public MinAggregator(String attributePath) {
        super(attributePath);
    }

    @Override
    public void accumulate(I entry) {
        R extractedValue = (R) extract(entry);

        if (isCurrentlyGreaterThan(extractedValue)) {
            min = extractedValue;
        }
    }

    private boolean isCurrentlyGreaterThan(R otherValue) {
        return min == null || min.compareTo(otherValue) > 0;
    }

    @Override
    public void combine(Aggregator aggregator) {
        MinAggregator maxAggregator = (MinAggregator) aggregator;
        R valueFromOtherAggregator = (R) maxAggregator.min;
        if (isCurrentlyGreaterThan(valueFromOtherAggregator)) {
            this.min = valueFromOtherAggregator;
        }
    }

    @Override
    public R aggregate() {
        return min;
    }
}
