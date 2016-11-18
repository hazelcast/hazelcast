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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DistinctValuesAggregator<R, K, V> extends AbstractAggregator<Set<R>, K, V> {
    Set<R> values = new HashSet<R>();

    public DistinctValuesAggregator() {
        super();
    }

    public DistinctValuesAggregator(String attributePath) {
        super(attributePath);
    }

    @Override
    public void accumulate(Map.Entry<K, V> entry) {
        R extractedValue = (R) extract(entry);
        values.add(extractedValue);
    }

    @Override
    public void combine(Aggregator aggregator) {
        DistinctValuesAggregator distinctValuesAggregator = (DistinctValuesAggregator) aggregator;
        this.values.addAll(distinctValuesAggregator.values);
    }

    @Override
    public Set<R> aggregate() {
        return values;
    }
}
