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

package com.hazelcast.cardinality.impl.hyperloglog.impl;

import com.hazelcast.cardinality.impl.hyperloglog.HyperLogLog;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 1. http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf
 */
@SuppressWarnings("checkstyle:magicnumber")
public class SparseHyperLogLog
        extends AbstractHyperLogLog {

    private static final long P_PRIME_FENCE_MASK = 0x8000000000L;

    // [1] Shows good cardinality estimation for the sparse range of sets. P_PRIME within [p .. 25]
    private static final long P_PRIME = 25;
    private static final long P_PRIME_MASK = (1 << P_PRIME) - 1;

    private long pMask;
    private long pDiffMask;
    private long pDiffEncodedMask;
    private long encodedPDiffShiftBits;

    private final Map<Integer, Integer> sparseSet = new HashMap<Integer, Integer>();
    private int[] temp;
    private int defaultTempSetCapacity;
    private int sparseToDenseThreshold;
    private int mPrime;
    private int tempIdx;

    public SparseHyperLogLog(final int p) {
        this(null, p);
    }

    public SparseHyperLogLog(final IHyperLogLogCompositeContext ctx, final int p) {
        super(ctx, p);
    }

    protected void init(int p) {
        super.init(p);
        this.mPrime = 1 << P_PRIME;
        this.sparseToDenseThreshold = 40000;
        this.defaultTempSetCapacity = (int) (sparseToDenseThreshold * .25);
        this.temp = new int[this.defaultTempSetCapacity];

        this.pMask = ((1 << p) - 1);
        this.pDiffMask = P_PRIME_MASK ^ pMask;
        this.pDiffEncodedMask = (1L << (P_PRIME - p)) - 1;
        this.encodedPDiffShiftBits = (int) (32 - (P_PRIME - p));
    }

    @Override
    public boolean aggregate(long hash) {
        int encoded = encodeHash(hash);
        temp[tempIdx++] = encoded;
        boolean isTempAtCapacity = tempIdx == defaultTempSetCapacity;
        if (isTempAtCapacity) {
            mergeAndResetTmp();
            convertToDenseIfNeeded();
        }

        invalidateCachedEstimate();
        return true;
    }

    @Override
    public long estimate() {
        Long cachedEstimate = getCachedEstimate();
        if (cachedEstimate == null) {
            mergeAndResetTmp();
            return cacheAndGetLastEstimate(linearCounting(mPrime, mPrime - sparseSet.size()));
        }

        return cachedEstimate;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        mergeAndResetTmp();
        out.writeInt(p);
        out.writeInt(sparseSet.size());
        for (Map.Entry<Integer, Integer> entry : sparseSet.entrySet()) {
            out.writeInt(entry.getKey());
            out.writeInt(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        init(in.readInt());
        int sz = in.readInt();
        for (int i = 0; i < sz; i++) {
            sparseSet.put(in.readInt(), in.readInt());
        }
    }

    @Override
    public HyperLogLogEncType getEncodingType() {
        return HyperLogLogEncType.SPARSE;
    }

    private int encodeHash(long hash) {
        int index = (int) (hash & P_PRIME_MASK) << (32 - P_PRIME);
        if ((hash & pDiffMask) == 0) {
            return index | Long.numberOfTrailingZeros((hash >> P_PRIME) | P_PRIME_FENCE_MASK) | 0x1;
        }

        return index & ~1;
    }

    private int extractDenseIndexOf(long hash) {
        return (int) ((hash >> (32 - P_PRIME)) & pMask);
    }

    private byte extractDenseNumOfZerosOf(long hash) {
        if ((hash & 0x1) == 0) {
            int pDiff = (int) ((hash >> encodedPDiffShiftBits) & pDiffEncodedMask);
            return (byte) (Integer.numberOfTrailingZeros(pDiff) + 1);
        }

        return (byte) ((hash & ((1 << (32 - P_PRIME)) - 1) >> 1) + (P_PRIME - p) + 1);
    }

    private void mergeAndResetTmp() {
        for (int i = 0; i < tempIdx; i++) {
            int hash = temp[i];
            int sparseIndex = (int) ((hash >> (32 - P_PRIME) & P_PRIME_MASK) & mPrime - 1);
            Integer oldHashValue = sparseSet.get(sparseIndex);
            if (oldHashValue == null) {
                sparseSet.put(sparseIndex, hash);
            } else if (oldHashValue != hash) {
                int oldRunOfTrailingZeros = extractDenseNumOfZerosOf(oldHashValue);
                int newRunOfTrailingZeros = extractDenseNumOfZerosOf(hash);
                if (newRunOfTrailingZeros > oldRunOfTrailingZeros) {
                    sparseSet.put(sparseIndex, hash);
                }
            }
        }

        Arrays.fill(temp, 0);
        tempIdx = 0;
    }

    private void convertToDenseIfNeeded() {

    }

    private HyperLogLog asDense() {
        byte[] register = new byte[m];
        for (int hash : sparseSet.values()) {
            int index = extractDenseIndexOf(hash) & (register.length - 1);
            register[index] = (byte) Math.max(register[index], extractDenseNumOfZerosOf(hash));
        }

        return new DenseHyperLogLog(p, register);
    }

}



