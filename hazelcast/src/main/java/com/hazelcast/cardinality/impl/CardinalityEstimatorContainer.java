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

package com.hazelcast.cardinality.impl;

import com.hazelcast.cardinality.impl.hyperloglog.HyperLogLog;
import com.hazelcast.cardinality.impl.hyperloglog.impl.HyperLogLogEncType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class CardinalityEstimatorContainer implements DataSerializable {

    private static final int DEFAULT_HLL_PRECISION = 14;

    private HyperLogLog hll;

    public CardinalityEstimatorContainer() {
        hll = HyperLogLogEncType.COMPO.build(DEFAULT_HLL_PRECISION);
    }

    public HyperLogLog getStore() {
        return hll;
    }

    public void setStore(HyperLogLog hll) {
        this.hll = hll;
    }

    public boolean aggregate(long hash) {
        return hll.aggregate(hash);
    }

    public boolean aggregateAll(long[] hashes) {
        return hll.aggregateAll(hashes);
    }

    public long estimate() {
        return hll.estimate();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(hll.getEncodingType().name());
        hll.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        HyperLogLog store = HyperLogLogEncType.valueOf(in.readUTF()).build(DEFAULT_HLL_PRECISION);
        store.readData(in);
        hll = store;
    }
}
