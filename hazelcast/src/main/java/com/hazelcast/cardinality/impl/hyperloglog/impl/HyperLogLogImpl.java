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

import static com.hazelcast.cardinality.impl.hyperloglog.impl.HyperLogLogEncoding.SPARSE;

public class HyperLogLogImpl implements HyperLogLog {

    private static final int LOWER_P_BOUND = 4;
    private static final int UPPER_P_BOUND = 16;

    // [1] Shows good cardinality estimation
    private static final int DEFAULT_P = 14;
    private static final int DEFAULT_P_PRIME = 25;
    private static final int SPARSE_TO_DENSE_THRESHOLD = 40000;

    private Long cachedEstimate;
    private HyperLogLogEncoder encoder;

    public HyperLogLogImpl() {
        this(DEFAULT_P, DEFAULT_P_PRIME);
    }

    public HyperLogLogImpl(final int p, final int pPrime) {
        if (p < LOWER_P_BOUND || p > UPPER_P_BOUND) {
            throw new IllegalArgumentException("Precision (p) outside valid range [4..16].");
        }

        this.encoder = new SparseHyperLogLogEncoder(p, pPrime);
    }

    @Override
    public long estimate() {
        if (cachedEstimate == null) {
            cachedEstimate = encoder.estimate();
            convertToDenseIfNeeded();
        }

        return cachedEstimate;
    }

    @Override
    public boolean aggregate(long hash) {
        boolean changed = encoder.aggregate(hash);
        if (changed) {
            cachedEstimate = null;
        }

        return changed;
    }

    @Override
    public boolean aggregateAll(long[] hashes) {
        boolean changed = false;
        for (long hash : hashes) {
            changed |= aggregate(hash);
        }

        return changed;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(encoder.getEncodingType().name());
        encoder.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        encoder = HyperLogLogEncoding.valueOf(in.readUTF()).build();
        encoder.readData(in);
    }

    private void convertToDenseIfNeeded() {
        boolean shouldConvertToDense = encoder.getEncodingType() == SPARSE
                && encoder.getMemoryFootprint() > SPARSE_TO_DENSE_THRESHOLD;
        
        if (shouldConvertToDense) {
            encoder = ((SparseHyperLogLogEncoder) encoder).asDense();
        }
    }

}
