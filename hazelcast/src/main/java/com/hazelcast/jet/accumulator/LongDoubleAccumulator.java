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
 * Mutable container of a {@code long} and a {@code double} value.
 *
 * @since Jet 3.0
 */
public class LongDoubleAccumulator {

    private long longValue;
    private double doubleValue;

    /**
     * Creates a new instance with values equal to 0.
     */
    public LongDoubleAccumulator() {
    }

    /**
     * Creates a new instance with the specified values.
     */
    public LongDoubleAccumulator(long longValue, double doubleValue) {
        this.longValue = longValue;
        this.doubleValue = doubleValue;
    }

    /**
     * Returns the {@code long} value.
     */
    public long getLong() {
        return longValue;
    }

    /**
     * Sets the {@code long} value.
     */
    public void setLong(long value1) {
        this.longValue = value1;
    }

    /**
     * Returns the {@code double} value.
     */
    public double getDouble() {
        return doubleValue;
    }

    /**
     * Sets the {@code double} value.
     */
    public void setDouble(double value2) {
        this.doubleValue = value2;
    }

    @Override
    public boolean equals(Object o) {
        return this == o ||
                o != null
                && this.getClass() == o.getClass()
                && this.longValue == ((LongDoubleAccumulator) o).longValue
                && Double.compare(this.doubleValue, ((LongDoubleAccumulator) o).doubleValue) == 0;
    }

    @Override
    public int hashCode() {
        int result;
        result = (int) (longValue ^ (longValue >>> 32));
        long temp = Double.doubleToLongBits(doubleValue);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LongLongAccumulator(" + longValue + ", " + doubleValue + ')';
    }
}
