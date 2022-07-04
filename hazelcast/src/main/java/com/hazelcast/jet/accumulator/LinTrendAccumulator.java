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

import com.hazelcast.jet.JetException;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Objects;

/**
 * Maintains the components needed to compute the linear regression on a
 * set of {@code (long, long)} pairs. The intermediate results are held in
 * the form of {@link BigInteger} and the finished value is a {@code
 * double}-valued linear coefficient.
 *
 * @since Jet 3.0
 */
public final class LinTrendAccumulator {
    private static final int MAX_BIGINT_LEN = 255;

    private long n;
    private BigInteger sumX;
    private BigInteger sumY;
    private BigInteger sumXY;
    private BigInteger sumX2;

    /**
     * Constructs a new accumulator with all components at zero.
     */
    public LinTrendAccumulator() {
        this.sumX = BigInteger.ZERO;
        this.sumY = BigInteger.ZERO;
        this.sumXY = BigInteger.ZERO;
        this.sumX2 = BigInteger.ZERO;
    }

    /**
     * Creates a new accumulator with the given components. Intended only for
     * testing and deserialization.
     */
    public LinTrendAccumulator(long n, BigInteger sumX, BigInteger sumY, BigInteger sumXY, BigInteger sumX2) {
        this.n = n;
        this.sumX = sumX;
        this.sumY = sumY;
        this.sumXY = sumXY;
        this.sumX2 = sumX2;
    }

    /**
     * Accumulates a new sample.
     */
    public LinTrendAccumulator accumulate(long x, long y) {
        n++;
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
    public LinTrendAccumulator combine(LinTrendAccumulator that) {
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
    public LinTrendAccumulator deduct(LinTrendAccumulator that) {
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
    public double export() {
        BigInteger bigN = BigInteger.valueOf(n);
        return bigN.multiply(sumXY).subtract(sumX.multiply(sumY)).doubleValue() /
                bigN.multiply(sumX2).subtract(sumX.multiply(sumX)).doubleValue();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LinTrendAccumulator)) {
            return false;
        }
        LinTrendAccumulator that = (LinTrendAccumulator) obj;
        return this.n == that.n
                && Objects.equals(this.sumX, that.sumX)
                && Objects.equals(this.sumY, that.sumY)
                && Objects.equals(this.sumXY, that.sumXY)
                && Objects.equals(this.sumX2, that.sumX2);
    }

    @Override
    public int hashCode() {
        int hc = 17;
        hc = 73 * hc + Long.hashCode(n);
        hc = 73 * hc + Objects.hashCode(sumX);
        hc = 73 * hc + Objects.hashCode(sumY);
        hc = 73 * hc + Objects.hashCode(sumXY);
        hc = 73 * hc + Objects.hashCode(sumX2);
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
