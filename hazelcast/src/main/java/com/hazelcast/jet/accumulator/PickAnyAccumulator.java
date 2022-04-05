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

package com.hazelcast.jet.accumulator;

import com.hazelcast.jet.aggregate.AggregateOperations;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Keeps the state needed to implement the {@link
 * AggregateOperations#pickAny()} aggregate operation. It maintains the
 * count of accumulated items so that it can properly set its value to
 * {@code null} when all the items are deducted.
 *
 * @param <T>
 * @since Jet 4.5
 */
public class PickAnyAccumulator<T> {

    private T value;
    private long count;

    /**
     * Constructs an empty {@code pickAny} accumulator.
     */
    public PickAnyAccumulator() {
    }

    /**
     * Constructs a {@code pickAny} accumulator with the full state passed in
     * the parameters.
     *
     * @param value the picked object
     * @param count the count of accumulated objects
     */
    public PickAnyAccumulator(T value, long count) {
        this.value = value;
        this.count = count;
    }

    /**
     * Accumulates an item.
     */
    public void accumulate(@Nullable T t) {
        if (t == null) {
            return;
        }
        if (value == null) {
            value = t;
        }
        count++;
    }

    /**
     * Combines another accumulator with this one.
     */
    public void combine(@Nonnull PickAnyAccumulator<T> other) {
        count += other.count;
        if (value == null) {
            value = other.value;
        }
    }

    /**
     * Deducts another accumulator from this one.
     */
    public void deduct(@Nonnull PickAnyAccumulator<T> other) {
        count -= other.count;
        assert count >= 0 : "Negative count after deduct";
        if (count == 0) {
            value = null;
        }
    }

    /**
     * Returns the picked object.
     */
    public T get() {
        return value;
    }

    /**
     * Returns the count of accumulated objects.
     */
    public long count() {
        return count;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean equals(Object o) {
        PickAnyAccumulator that;
        return this == o ||
                o != null
                        && this.getClass() == o.getClass()
                        && this.count == (that = (PickAnyAccumulator) o).count
                        && Objects.equals(this.value, that.value);
    }

    @Override
    public int hashCode() {
        long hc = 17;
        hc = 73 * hc + count;
        hc = 73 * hc + (value != null ? value.hashCode() : 0);
        return Long.hashCode(hc);
    }

    @Override
    public String toString() {
        return "MutableReference(" + value + ')';
    }
}
