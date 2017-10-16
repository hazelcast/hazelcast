/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
 * Mutable container of a {@code long} and a {@code double} value.
 */
public class LongDoubleAccumulator {

    private long value1;
    private double value2;

    /**
     * Creates a new instance with values equal to 0.
     */
    public LongDoubleAccumulator() {
    }

    /**
     * Creates a new instance with the specified value.
     */
    public LongDoubleAccumulator(long value1, double value2) {
        this.value1 = value1;
        this.value2 = value2;
    }

    /**
     * Returns the current value1.
     */
    public long getValue1() {
        return value1;
    }

    /**
     * Sets the value1.
     */
    public void setValue1(long value1) {
        this.value1 = value1;
    }

    /**
     * Returns the current value2.
     */
    public double getValue2() {
        return value2;
    }

    /**
     * Sets the value2.
     */
    public void setValue2(double value2) {
        this.value2 = value2;
    }

    @Override
    public boolean equals(Object o) {
        return this == o ||
                o != null
                && this.getClass() == o.getClass()
                && this.value1 == ((LongDoubleAccumulator) o).value1
                && this.value2 == ((LongDoubleAccumulator) o).value2;
    }

    @Override
    public int hashCode() {
        int result;
        result = (int) (value1 ^ (value1 >>> 32));
        long temp = Double.doubleToLongBits(value2);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LongLongAccumulator(" + value1 + ", " + value2 + ')';
    }
}
