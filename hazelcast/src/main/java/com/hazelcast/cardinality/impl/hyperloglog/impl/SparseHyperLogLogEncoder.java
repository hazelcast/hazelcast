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
public class SparseHyperLogLogEncoder implements HyperLogLogEncoder  {

    private static final long P_PRIME_FENCE_MASK = 0x8000000000L;
    private static final int SPARSE_SOFT_MAX_CAPACITY = 40000;

    private int p;
    private int pMask;
    private int pPrime;
    private int pPrimeMask;
    private long pDiffMask;
    private long pDiffEncodedMask;
    private long encodedPDiffShiftBits;

    private final Map<Integer, Integer> sparseSet = new HashMap<Integer, Integer>();
    private int[] temp;
    private int defaultTempSetCapacity;
    private int mPrime;
    private int tempIdx;

    SparseHyperLogLogEncoder() {
    }

    public SparseHyperLogLogEncoder(final int p, final int pPrime) {
        init(p, pPrime);
    }

    public void init(int p, int pPrime) {
        this.p = p;
        this.pPrime = pPrime;
        this.pPrimeMask = (1 << pPrime) - 1;
        this.mPrime = 1 << pPrime;
        this.defaultTempSetCapacity = (int) (SPARSE_SOFT_MAX_CAPACITY * .25);
        this.temp = new int[this.defaultTempSetCapacity];

        this.pMask = ((1 << p) - 1);
        this.pDiffMask = pPrimeMask ^ pMask;
        this.pDiffEncodedMask = (1L << (pPrime - p)) - 1;
        this.encodedPDiffShiftBits = 32 - (pPrime - p);
    }

    @Override
    public boolean aggregate(long hash) {
        int encoded = encodeHash(hash);
        temp[tempIdx++] = encoded;
        boolean isTempAtCapacity = tempIdx == defaultTempSetCapacity;
        if (isTempAtCapacity) {
            mergeAndResetTmp();
        }

        return true;
    }

    @Override
    public long estimate() {
        mergeAndResetTmp();
        return linearCounting(mPrime, mPrime - sparseSet.size());
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        mergeAndResetTmp();
        out.writeInt(p);
        out.writeInt(pPrime);
        out.writeInt(sparseSet.size());
        for (Map.Entry<Integer, Integer> entry : sparseSet.entrySet()) {
            out.writeInt(entry.getKey());
            out.writeInt(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        init(in.readInt(), in.readInt());
        int sz = in.readInt();
        for (int i = 0; i < sz; i++) {
            sparseSet.put(in.readInt(), in.readInt());
        }
    }

    @Override
    public HyperLogLogEncoding getEncodingType() {
        return HyperLogLogEncoding.SPARSE;
    }

    @Override
    public int getMemoryFootprint() {
        // For absolute correctness this should be based on the actual mem footprint
        // of the sparse layout vs dense layout.
        // TODO @tkountis, upcoming fix to address this, with a compressed solution.
        return sparseSet.size();
    }

    public HyperLogLogEncoder asDense() {
        byte[] register = new byte[1 << this.p];
        for (int hash : sparseSet.values()) {
            int index = extractDenseIndexOf(hash) & (register.length - 1);
            register[index] = (byte) Math.max(register[index], extractNumOfZerosOf(hash));
        }

        return new DenseHyperLogLogEncoder(p, register);
    }

    private int encodeHash(long hash) {
        int index = (int) (hash & pPrimeMask) << (32 - pPrime);
        if ((hash & pDiffMask) == 0) {
            return index | Long.numberOfTrailingZeros((hash >> pPrime) | P_PRIME_FENCE_MASK) | 0x1;
        }

        return index & ~1;
    }

    private int extractDenseIndexOf(long hash) {
        return (int) ((hash >> (32 - pPrime)) & pMask);
    }

    private byte extractNumOfZerosOf(long hash) {
        if ((hash & 0x1) == 0) {
            int pDiff = (int) ((hash >> encodedPDiffShiftBits) & pDiffEncodedMask);
            return (byte) (Integer.numberOfTrailingZeros(pDiff) + 1);
        }

        return (byte) ((hash & ((1 << (32 - pPrime)) - 1) >> 1) + (pPrime - p) + 1);
    }

    private long linearCounting(final int total, final int empty) {
        return (long) (total * Math.log(total / (double) empty));
    }

    private void mergeAndResetTmp() {
        for (int i = 0; i < tempIdx; i++) {
            int hash = temp[i];
            int sparseIndex = (hash >> (32 - pPrime) & pPrimeMask) & mPrime - 1;
            Integer oldHashValue = sparseSet.get(sparseIndex);
            if (oldHashValue == null) {
                sparseSet.put(sparseIndex, hash);
            } else if (oldHashValue != hash) {
                int oldRunOfTrailingZeros = extractNumOfZerosOf(oldHashValue);
                int newRunOfTrailingZeros = extractNumOfZerosOf(hash);
                if (newRunOfTrailingZeros > oldRunOfTrailingZeros) {
                    sparseSet.put(sparseIndex, hash);
                }
            }
        }

        Arrays.fill(temp, 0);
        tempIdx = 0;
    }

}



