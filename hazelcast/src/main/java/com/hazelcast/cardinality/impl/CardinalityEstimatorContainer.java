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
import com.hazelcast.cardinality.impl.hyperloglog.impl.HyperLogLogImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class CardinalityEstimatorContainer implements IdentifiedDataSerializable {

    private HyperLogLog hll;

    public CardinalityEstimatorContainer() {
        hll = new HyperLogLogImpl();
    }

    public void add(long hash) {
        hll.add(hash);
    }

    public long estimate() {
        return hll.estimate();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(hll);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        hll = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return CardinalityEstimatorDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CardinalityEstimatorDataSerializerHook.CARDINALITY_EST_CONTAINER;
    }
}
