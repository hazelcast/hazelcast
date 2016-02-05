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

package com.hazelcast.jet.impl.counters;

import com.hazelcast.jet.spi.counters.Accumulator;
import com.hazelcast.jet.spi.counters.SimpleAccumulator;

public class LongCounter implements SimpleAccumulator<Long> {
    private static final long serialVersionUID = 1L;
    private long localValue;

    public LongCounter() {
    }

    public LongCounter(long value) {
        this.localValue = value;
    }

    public long getPrimitiveValue() {
        return this.localValue;
    }

    @Override
    public void add(Long value) {
        this.localValue += value;
    }

    @Override
    public Long getLocalValue() {
        return this.localValue;
    }

    @Override
    public void merge(Accumulator<Long, Long> other) {
        this.localValue += other.getLocalValue();
    }

    @Override
    public void resetLocal() {
        this.localValue = 0;
    }

    public void add(long value) {
        this.localValue += value;
    }

    @Override
    public String toString() {
        return "LongCounter " + this.localValue;
    }
}
