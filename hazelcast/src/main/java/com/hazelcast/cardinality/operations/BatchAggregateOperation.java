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
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.Arrays;

public class BatchAggregateOperation
        extends CardinalityEstimatorBackupAwareOperation {

    private long[] values;
    private boolean changed;

    public BatchAggregateOperation() { }

    public BatchAggregateOperation(String name, long[] values) {
        super(name);
        this.values = Arrays.copyOf(values, values.length);
    }

    @Override
    public int getId() {
        return CardinalityEstimatorDataSerializerHook.BATCH_AGGREGATE;
    }

    @Override
    public void run() throws Exception {
        changed = getCardinalityEstimatorContainer().aggregateAll(values);
    }

    @Override
    public Object getResponse() {
        return changed;
    }

    @Override
    public Operation getBackupOperation() {
        return new AggregateBackupOperation(name, values);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLongArray(values);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        values = in.readLongArray();
    }
}
