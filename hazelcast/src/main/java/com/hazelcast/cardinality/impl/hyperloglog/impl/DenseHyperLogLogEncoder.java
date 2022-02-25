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

package com.hazelcast.cardinality.impl.hyperloglog.impl;

import com.hazelcast.cardinality.impl.CardinalityEstimatorDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static com.hazelcast.cardinality.impl.hyperloglog.impl.HyperLogLogEncoding.SPARSE;

/**
 * 1. http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
 * 2. http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf
 */
@SuppressWarnings("checkstyle:magicnumber")
public class DenseHyperLogLogEncoder implements HyperLogLogEncoder {

    private int p;
    private byte[] register;
    private transient int numOfEmptyRegs;
    private transient double[] invPowLookup;
    private transient int m;
    private transient long pFenseMask;

    public DenseHyperLogLogEncoder() {
    }

    public DenseHyperLogLogEncoder(final int p) {
        this(p, null);
    }

    public DenseHyperLogLogEncoder(final int p, final byte[] register) {
        this.init(p, register);
    }

    private void init(final int p, final byte[] register) {
        this.p = p;
        this.m = 1 << p;
        this.numOfEmptyRegs = m;
        this.register = register != null ? register : new byte[m];
        this.invPowLookup = new double[64 - p + 1];
        this.pFenseMask = 1 << (64 - p) - 1;
        this.prePopulateInvPowLookup();
    }

    @Override
    public boolean add(long hash) {
        final int index = (int) hash & (register.length - 1);
        final int value = Long.numberOfTrailingZeros((hash >>> p) | pFenseMask) + 1;

        assert index < register.length;
        assert value <= (1 << 8) - 1;
        assert value <= 64 - p;

        if (value > register[index]) {
            register[index] = (byte) value;
            return true;
        }

        return false;
    }

    @Override
    public long estimate() {
        final double raw = (1 / computeE()) * alpha() * m * m;
        return applyRangeCorrection(raw);
    }

    @Override
    public HyperLogLogEncoder merge(HyperLogLogEncoder encoder) {
        DenseHyperLogLogEncoder otherDense;
        if (SPARSE.equals(encoder.getEncodingType())) {
            otherDense = (DenseHyperLogLogEncoder) ((SparseHyperLogLogEncoder) encoder).asDense();
        } else {
            otherDense = (DenseHyperLogLogEncoder) encoder;
        }

        for (int i = 0; i < register.length; i++) {
            register[i] = (byte) Math.max(register[i], otherDense.register[i]);
        }

        return this;
    }

    @Override
    public int getFactoryId() {
        return CardinalityEstimatorDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CardinalityEstimatorDataSerializerHook.HLL_DENSE_ENC;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(p);
        out.writeByteArray(register);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        init(in.readInt(), null);
        this.register = in.readByteArray();
    }

    @Override
    public int getMemoryFootprint() {
        return m;
    }

    @Override
    public HyperLogLogEncoding getEncodingType() {
        return HyperLogLogEncoding.DENSE;
    }

    private double alpha() {
        // make sure m is always >= 16 for p = 4 -> m = 16
        // if p ∈ [4..16] as of [1]
        assert m >= 16;

        if (m >= 128) {
            return .7213 / (1 + 1.079 / m);
        }
        if (m == 64) {
            return .709;
        }
        if (m == 32) {
            return .697;
        }
        if (m == 16) {
            return .673;
        }
        return -1;
    }

    private long applyRangeCorrection(double e) {
        double ePrime = e <= m * 5 ? (e - estimateBias(e)) : e;
        double h = numOfEmptyRegs != 0 ? linearCounting(m, numOfEmptyRegs) : ePrime;
        return (long) (exceedsThreshold(h) ? ePrime : h);
    }

    private double computeE() {
        double e = 0;
        numOfEmptyRegs = 0;
        for (byte r : register) {
            if (r > 0) {
                e += invPow(r);
            } else {
                numOfEmptyRegs++;
            }
        }
        return e + numOfEmptyRegs;
    }

    /**
     * [2] We use k nearest neighbor interpolation to get the bias for a given raw estimate
     * The choice of k = 6 is rather arbitrary. The best value of k could be determined experimentally,
     * but we found that the choice has only a minuscule influence.
     */
    private long estimateBias(double e) {
        int i = 0;
        double[] rawEstimates = DenseHyperLogLogConstants.RAW_ESTIMATE_DATA[p - 4];
        double closestToZero = Math.abs(e - rawEstimates[0]);
        NavigableMap<Double, Integer> distances = new TreeMap<Double, Integer>();
        for (double est : rawEstimates) {
            double distance = e - est;
            distances.put(distance, i++);
            if (Math.abs(distance) < closestToZero) {
                closestToZero = distance;
            }
        }

        // abomination to compute kNN elements (we could ideally use a tree structure)
        int kNN = 6;
        double sum = 0;
        Iterator<Map.Entry<Double, Integer>> firstX = distances.descendingMap().tailMap(closestToZero).entrySet().iterator();
        Iterator<Map.Entry<Double, Integer>> lastX = distances.tailMap(closestToZero).entrySet().iterator();

        int kNNLeft = kNN;
        while (kNNLeft-- > kNN / 2 && firstX.hasNext()) {
            sum += DenseHyperLogLogConstants.BIAS_DATA[p - 4][firstX.next().getValue()];
        }

        while (kNNLeft-- >= 0 && lastX.hasNext()) {
            sum += DenseHyperLogLogConstants.BIAS_DATA[p - 4][lastX.next().getValue()];
        }

        return (long) (sum / kNN);
    }

    private boolean exceedsThreshold(double e) {
        return e >= DenseHyperLogLogConstants.THRESHOLD[p - 4];
    }

    private double invPow(int index) {
        assert index <= 64 - p;
        return invPowLookup[index];
    }

    private long linearCounting(final int total, final int empty) {
        return (long) (total * Math.log(total / (double) empty));
    }

    private void prePopulateInvPowLookup() {
        invPowLookup[0] = 1;
        for (int i = 1; i <= (64 - p); i++) {
            invPowLookup[i] = Math.pow(2, -i);
        }
    }
}
