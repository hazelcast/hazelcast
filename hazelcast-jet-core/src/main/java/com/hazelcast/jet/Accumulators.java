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

package com.hazelcast.jet;

import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Objects;

/**
 * Mutable holders of primitive values and references with support
 * for some common accumulation operations.
 */
public final class Accumulators {

    private Accumulators() {
    }

    /**
     * Accumulator of a primitive {@code long} value.
     */
    public static class LongAccumulator {

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
         * Uses {@link Math#addExact(long, long) Math.addExact()} to add the
         * supplied value to this accumulator.
         */
        public LongAccumulator addExact(long value) {
            this.value = Math.addExact(this.value, value);
            return this;
        }

        /**
         * Uses {@link Math#addExact(long, long) Math.addExact()} to add the value
         * of the supplied accumulator into this one.
         */
        public LongAccumulator addExact(LongAccumulator that) {
            this.value = Math.addExact(this.value, that.value);
            return this;
        }

        /**
         * Subtracts the value of the supplied accumulator from this one.
         */
        public LongAccumulator subtract(LongAccumulator that) {
            this.value -= that.value;
            return this;
        }

        /**
         * Uses {@link Math#subtractExact(long, long) Math.subtractExact()}
         * to subtract the value of the supplied accumulator from this one.
         */
        public LongAccumulator subtractExact(LongAccumulator that) {
            this.value = Math.subtractExact(this.value, that.value);
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
            return "MutableLong(" + value + ')';
        }
    }

    /**
     * Accumulator of a primitive {@code double} value.
     */
    public static class DoubleAccumulator {

        private double value;

        /**
         * Creates a new instance with {@code value == 0}.
         */
        public DoubleAccumulator() {
        }

        /**
         * Creates a new instance with the specified value.
         */
        public DoubleAccumulator(double value) {
            this.value = value;
        }

        /**
         * Returns the current value.
         */
        public double get() {
            return value;
        }

        /**
         * Sets the value as given.
         */
        public DoubleAccumulator set(double value) {
            this.value = value;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            return this == o ||
                    o != null
                    && this.getClass() == o.getClass()
                    && this.value == ((DoubleAccumulator) o).value;
        }

        @Override
        public int hashCode() {
            return Double.hashCode(value);
        }

        @Override
        public String toString() {
            return "MutableDouble(" + value + ')';
        }
    }

    /**
     * Mutable container of an object reference.
     *
     * @param <T> referenced object type
     */
    public static class MutableReference<T> {

        /**
         * The holder's value.
         */
        private T value;

        /**
         * Creates a new instance with a {@code null} value.
         */
        public MutableReference() {
        }

        /**
         * Creates a new instance with the specified value.
         */
        public MutableReference(T value) {
            this.value = value;
        }

        /**
         * Returns the current value.
         */
        public T get() {
            return value;
        }

        /**
         * Sets the value as given.
         */
        public MutableReference set(T value) {
            this.value = value;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            return this == o ||
                    o != null
                    && this.getClass() == o.getClass()
                    && Objects.equals(this.value, ((MutableReference) o).value);
        }

        @Override
        public int hashCode() {
            return value != null ? value.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "MutableReference(" + value + ')';
        }
    }

    /**
     * Maintains the components needed to compute the linear regression on a
     * set of {@code (long, long)} pairs. The intermediate results are held in
     * the form of {@link BigInteger} and the finished value is a {@code
     * double}-valued linear coefficient.
     */
    public static final class LinRegAccumulator {
        private static final int MAX_BIGINT_LEN = 255;

        private long n;
        private BigInteger sumX;
        private BigInteger sumY;
        private BigInteger sumXY;
        private BigInteger sumX2;

        /**
         * Constructs a new accumulator with all components at zero.
         */
        public LinRegAccumulator() {
            this.sumX = BigInteger.ZERO;
            this.sumY = BigInteger.ZERO;
            this.sumXY = BigInteger.ZERO;
            this.sumX2 = BigInteger.ZERO;
        }

        /**
         * Creates a new accumulator with the given components. Intended only for
         * testing and deserialization.
         */
        public LinRegAccumulator(long n, BigInteger sumX, BigInteger sumY, BigInteger sumXY, BigInteger sumX2) {
            this.n = n;
            this.sumX = sumX;
            this.sumY = sumY;
            this.sumXY = sumXY;
            this.sumX2 = sumX2;
        }

        /**
         * Accumulates a new sample.
         */
        public LinRegAccumulator accumulate(long x, long y) {
            BigInteger bigX = BigInteger.valueOf(x);
            BigInteger bigY = BigInteger.valueOf(y);
            sumX = sumX.add(bigX);
            sumY = sumY.add(bigY);
            sumXY = sumXY.add(bigX.multiply(bigY));
            sumX2 = sumX2.add(bigX.multiply(bigX));
            return this;
        }

        /**
         * Combines this accumulator with the supplied one.
         */
        public LinRegAccumulator combine(LinRegAccumulator that) {
            n += that.n;
            sumX = sumX.add(that.sumX);
            sumY = sumY.add(that.sumY);
            sumXY = sumXY.add(that.sumXY);
            sumX2 = sumX2.add(that.sumX2);
            return this;
        }

        /**
         * Deducts the supplied accumulator from this one.
         */
        public LinRegAccumulator deduct(LinRegAccumulator that) {
            n -= that.n;
            sumX = sumX.subtract(that.sumX);
            sumY = sumY.subtract(that.sumY);
            sumXY = sumXY.subtract(that.sumXY);
            sumX2 = sumX2.subtract(that.sumX2);
            return this;
        }

        /**
         * Computes the linear coefficient of the linear regression of the
         * accumulated samples.
         */
        public double finish() {
            BigInteger bigN = BigInteger.valueOf(n);
            return bigN.multiply(sumXY).subtract(sumX.multiply(sumY)).doubleValue() /
                    bigN.multiply(sumX2).subtract(sumX.multiply(sumX)).doubleValue();
        }

        @Override
        public boolean equals(Object obj) {
            LinRegAccumulator that;
            return this == obj ||
                    obj instanceof LinRegAccumulator
                    && this.n == (that = (LinRegAccumulator) obj).n
                    && this.sumX.equals(that.sumX)
                    && this.sumY.equals(that.sumY)
                    && this.sumXY.equals(that.sumXY)
                    && this.sumX2.equals(that.sumX2);
        }

        @Override
        public int hashCode() {
            int hc = 17;
            hc = 73 * hc + Long.hashCode(n);
            hc = 73 * hc + sumX.hashCode();
            hc = 73 * hc + sumY.hashCode();
            hc = 73 * hc + sumXY.hashCode();
            hc = 73 * hc + sumX2.hashCode();
            return hc;
        }

        /**
         * Serializes this accumulator.
         */
        public void writeObject(ObjectDataOutput out) throws IOException {
            out.writeLong(n);
            writeBytes(out, sumX.toByteArray());
            writeBytes(out, sumY.toByteArray());
            writeBytes(out, sumXY.toByteArray());
            writeBytes(out, sumX2.toByteArray());
        }

        private static void writeBytes(ObjectDataOutput out, byte[] bytes) throws IOException {
            if (bytes.length > MAX_BIGINT_LEN) {
                throw new JetException(
                        "BigInteger serialized to " + bytes.length + " bytes, only up to 255 is supported");
            }
            out.write(bytes.length);
            out.write(bytes);
        }
    }
}
