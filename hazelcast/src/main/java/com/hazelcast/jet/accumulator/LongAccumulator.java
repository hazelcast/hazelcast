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

/**
 * Mutable container of a {@code long} value.
 *
 * @since Jet 3.0
 */
public class LongAccumulator {

    private long value;

    /**
     * Creates a new instance with {@code value == 0}.
     */
    public LongAccumulator() {
    }

    /**
     * Creates a new instance with the specified value.
     */
    public LongAccumulator(long value) {
        this.value = value;
    }

    /**
     * Returns the current value.
     */
    public long get() {
        return value;
    }

    /**
     * Sets the value as given.
     */
    public LongAccumulator set(long value) {
        this.value = value;
        return this;
    }

    /**
     * Adds the supplied value to this accumulator, throwing an exception
     * in the case of integer overflow.
     */
    public LongAccumulator add(long value) {
        this.value = Math.addExact(this.value, value);
        return this;
    }

    /**
     * Adds the value of the supplied accumulator to this accumulator, throwing
     * an exception in the case of integer overflow.
     */
    public LongAccumulator add(LongAccumulator that) {
        this.value = Math.addExact(this.value, that.value);
        return this;
    }

    /**
     * Subtracts the supplied value from this accumulator, throwing an
     * exception in the case of integer overflow.
     */
    public LongAccumulator subtract(long value) {
        this.value = Math.subtractExact(this.value, value);
        return this;
    }

    /**
     * Subtracts the value of the supplied accumulator from this one,
     * throwing an exception in the case of integer overflow.
     */
    public LongAccumulator subtract(LongAccumulator that) {
        this.value = Math.subtractExact(this.value, that.value);
        return this;
    }

    /**
     * Adds the supplied value to this accumulator, allowing integer overflow.
     * This removes the (tiny) overhead of overflow checking.
     */
    public LongAccumulator addAllowingOverflow(long value) {
        this.value += value;
        return this;
    }

    /**
     * Adds the value of the supplied accumulator to this accumulator, allowing
     * integer overflow. This removes the (tiny) overhead of overflow checking.
     */
    public LongAccumulator addAllowingOverflow(LongAccumulator that) {
        this.value += that.value;
        return this;
    }

    /**
     * Subtracts the supplied value from this accumulator, allowing integer
     * overflow. This removes the (tiny) overhead of overflow checking.
     */
    public LongAccumulator subtractAllowingOverflow(long value) {
        this.value -= value;
        return this;
    }

    /**
     * Subtracts the value of the supplied accumulator from this one, allowing
     * integer overflow. This removes the (tiny) overhead of overflow checking.
     */
    public LongAccumulator subtractAllowingOverflow(LongAccumulator that) {
        this.value -= that.value;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        return this == o ||
                o != null
                && this.getClass() == o.getClass()
                && this.value == ((LongAccumulator) o).value;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(value);
    }

    @Override
    public String toString() {
        return "LongAccumulator(" + value + ')';
    }
}
