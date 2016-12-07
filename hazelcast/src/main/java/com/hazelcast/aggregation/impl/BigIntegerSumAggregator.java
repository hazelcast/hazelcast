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

import java.math.BigInteger;

public class BigIntegerSumAggregator<I> extends AbstractAggregator<I, BigInteger> {

    private BigInteger sum = BigInteger.ZERO;

    public BigIntegerSumAggregator() {
        super();
    }

    public BigIntegerSumAggregator(String attributePath) {
        super(attributePath);
    }

    @Override
    public void accumulate(I entry) {
        BigInteger extractedValue = (BigInteger) extract(entry);
        sum = sum.add(extractedValue);
    }

    @Override
    public void combine(Aggregator aggregator) {
        BigIntegerSumAggregator longSumAggregator = (BigIntegerSumAggregator) aggregator;
        sum = sum.add(longSumAggregator.sum);
    }

    @Override
    public BigInteger aggregate() {
        return sum;
    }

}
