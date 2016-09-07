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

package com.hazelcast.cardinality.hyperloglog.impl;

import com.hazelcast.cardinality.hyperloglog.IHyperLogLog;
import com.hazelcast.cardinality.hyperloglog.IHyperLogLogContext;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * [1] http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf
 */
@SuppressWarnings("checkstyle:magicnumber")
public class HyperLogLogSparseStore
        extends AbstractHyperLogLog {

    private static final long P_PRIME_FENCE_MASK = 0x8000000000L;

    // [1] Shows good cardinality estimation for the sparse range of sets. P_PRIME within [p .. 25]
    private static final long P_PRIME = 25;

    private final long pPrimeMask = (1 << P_PRIME) - 1;
    private final long pMask;
    private final long pDiffMask;
    private final long pDiffEncodedMask;
    private final long encodedPDiffFence;
    private final long encodedPDiffShiftBits;

    private final Map<Integer, Integer> sparseSet = new HashMap<Integer, Integer>();
    private final int[] temp;
    private final int defaultTempSetCapacity;
    private final int sparseToDenseThreshold;
    private final int mPrime;
    private int tempIdx;

    public HyperLogLogSparseStore(final IHyperLogLogContext ctx, final int p) {
        super(ctx, p);
        this.mPrime = 1 << P_PRIME;
        this.sparseToDenseThreshold = 40000;
        this.defaultTempSetCapacity = (int) (sparseToDenseThreshold * .25);
        this.temp = new int[this.defaultTempSetCapacity];

        this.pMask = ((1 << p) - 1);
        this.pDiffMask = pPrimeMask ^ pMask;
        this.pDiffEncodedMask = ((1L << 32) - 1) ^ ((1L << 32 - (P_PRIME - p)) - 1);
        this.encodedPDiffFence = (1 << (P_PRIME - p));
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
            convertToDenseIfNeeded();
            return cacheAndGetLastEstimate(linearCounting(mPrime, mPrime - sparseSet.size()));
        }

        return cachedEstimate;
    }

    private int encodeHash(long hash) {
        int index = (int) (hash & pPrimeMask) << (32 - P_PRIME);
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
            //TODO @tkountis Simplify following bit shifting first AND mask then to lose the fence
            int pDiff = (int) (((hash & pDiffEncodedMask) >> encodedPDiffShiftBits) | encodedPDiffFence);
            return (byte) (Integer.numberOfTrailingZeros(pDiff) + 1);
        }

        return (byte) ((hash & ((1 << (32 - P_PRIME)) - 1) >> 1) + (P_PRIME - p) + 1);
    }

    private void mergeAndResetTmp() {
        for (int i = 0; i < tempIdx; i++) {
            int hash = temp[i];
            int sparseIndex = (int) ((hash >> (32 - P_PRIME) & pPrimeMask) & mPrime - 1);
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
        // For absolute correctness this should be based on the actual mem footprint of the sparse layout vs dense layout.
        boolean shouldConvertToDense = sparseSet.size() > sparseToDenseThreshold;
        if (shouldConvertToDense && getContext() != null) {
            switchStore(asDense());
        }
    }

    private IHyperLogLog asDense() {
        byte[] register = new byte[m];
        for (int hash : sparseSet.values()) {
            int index = extractDenseIndexOf(hash) & (register.length - 1);
            register[index] = (byte) Math.max(register[index], extractDenseNumOfZerosOf(hash));
        }

        return new HyperLogLogDenseStore(getContext(), p, register);
    }

}



