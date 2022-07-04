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
 * Mutable container of two {@code long} values.
 *
 * @since Jet 3.0
 */
public class LongLongAccumulator {

    private long value1;
    private long value2;

    /**
     * Creates a new instance with values equal to 0.
     */
    public LongLongAccumulator() {
    }

    /**
     * Creates a new instance with the specified values.
     */
    public LongLongAccumulator(long value1, long value2) {
        this.value1 = value1;
        this.value2 = value2;
    }

    /**
     * Returns the first value.
     */
    public long get1() {
        return value1;
    }

    /**
     * Sets the first value.
     */
    public void set1(long value1) {
        this.value1 = value1;
    }

    /**
     * Returns the second value.
     */
    public long get2() {
        return value2;
    }

    /**
     * Sets the second value.
     */
    public void set2(long value2) {
        this.value2 = value2;
    }

    /**
     * Adds the supplied amount to the first value.
     */
    public void add1(long amount) {
        this.value1 += amount;
    }

    /**
     * Adds the supplied amount to the second value.
     */
    public void add2(long amount) {
        this.value2 += amount;
    }

    @Override
    public boolean equals(Object o) {
        return this == o ||
                o != null
                && this.getClass() == o.getClass()
                && this.value1 == ((LongLongAccumulator) o).value1
                && this.value2 == ((LongLongAccumulator) o).value2;
    }

    @Override
    public int hashCode() {
        int result = (int) (value1 ^ (value1 >>> 32));
        result = 31 * result + (int) (value2 ^ (value2 >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LongLongAccumulator(" + value1 + ", " + value2 + ')';
    }
}
