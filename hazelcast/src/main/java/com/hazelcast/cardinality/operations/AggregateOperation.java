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

package com.hazelcast.cardinality.operations;

import com.hazelcast.cardinality.CardinalityEstimatorDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class AggregateOperation
        extends AbstractCardinalityEstimatorOperation {

    private long hash;

    public AggregateOperation() { }

    public AggregateOperation(String name, long hash) {
        super(name);
        this.hash = hash;
    }

    @Override
    public int getId() {
        return CardinalityEstimatorDataSerializerHook.AGGREGATE;
    }

    @Override
    public void run() throws Exception {
        getCardinalityEstimatorContainer().aggregate(hash);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(hash);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        hash = in.readLong();
    }
}
