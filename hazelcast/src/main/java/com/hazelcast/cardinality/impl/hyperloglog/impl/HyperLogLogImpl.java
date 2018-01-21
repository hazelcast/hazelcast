/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cardinality.impl.hyperloglog.HyperLogLog;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.cardinality.impl.hyperloglog.impl.HyperLogLogEncoding.SPARSE;

public class HyperLogLogImpl implements HyperLogLog {

    private static final int LOWER_P_BOUND = 4;
    private static final int UPPER_P_BOUND = 16;
    private static final int UPPER_P_PRIME_BOUND = 25;

    // [1] shows good cardinality estimation
    private static final int DEFAULT_P = 14;
    private static final int DEFAULT_P_PRIME = 25;

    private int m;
    private Long cachedEstimate;
    private HyperLogLogEncoder encoder;

    public HyperLogLogImpl() {
        this(DEFAULT_P, DEFAULT_P_PRIME);
    }

    public HyperLogLogImpl(final int p, final int pPrime) {
        if (p < LOWER_P_BOUND || p > UPPER_P_BOUND) {
            throw new IllegalArgumentException("Precision (p) outside valid range [4..16].");
        }

        if (pPrime < p || pPrime > UPPER_P_PRIME_BOUND) {
            throw new IllegalArgumentException("Prime precision (p') outside "
                    + "valid range [" + p + ".." + UPPER_P_PRIME_BOUND + "].");
        }

        this.m = 1 << p;
        this.encoder = new SparseHyperLogLogEncoder(p, pPrime);
    }

    @Override
    public long estimate() {
        if (cachedEstimate == null) {
            cachedEstimate = encoder.estimate();
        }

        return cachedEstimate;
    }

    @Override
    public void add(long hash) {
        convertToDenseIfNeeded();
        boolean changed = encoder.add(hash);
        if (changed) {
            cachedEstimate = null;
        }
    }

    @Override
    public void addAll(long[] hashes) {
        for (long hash : hashes) {
            add(hash);
        }
    }

    @Override
    public void merge(HyperLogLog other) {
        if (!(other instanceof HyperLogLogImpl)) {
            throw new IllegalStateException("Can't merge " + other + " into " + this);
        }

        encoder = encoder.merge(((HyperLogLogImpl) other).encoder);
        cachedEstimate = null;
    }

    @Override
    public int getFactoryId() {
        return CardinalityEstimatorDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CardinalityEstimatorDataSerializerHook.HLL;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(encoder);

        if (out.getVersion().isGreaterOrEqual(Versions.V3_10)) {
            out.writeInt(m);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        encoder = in.readObject();


        if (in.getVersion().isGreaterOrEqual(Versions.V3_10)) {
            m = in.readInt();
        }
    }

    private void convertToDenseIfNeeded() {
        boolean shouldConvertToDense = SPARSE.equals(encoder.getEncodingType())
                && encoder.getMemoryFootprint() >= m;
        if (shouldConvertToDense) {
            encoder = ((SparseHyperLogLogEncoder) encoder).asDense();
        }
    }

}
