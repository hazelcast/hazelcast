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

package com.hazelcast.jet.internal.impl.counters;

import com.hazelcast.jet.api.counters.Accumulator;
import com.hazelcast.jet.api.counters.SimpleAccumulator;

public class DoubleCounter implements SimpleAccumulator<Double> {
    private static final long serialVersionUID = 1L;
    private double localValue;

    @Override
    public void add(Double value) {
        if (value != null) {
            this.localValue += value;
        }
    }

    @Override
    public Double getLocalValue() {
        return this.localValue;
    }

    @Override
    public void resetLocal() {
        this.localValue = 0.0;
    }

    @Override
    public void merge(Accumulator<Double, Double> other) {
        this.localValue += other.getLocalValue();
    }
}
