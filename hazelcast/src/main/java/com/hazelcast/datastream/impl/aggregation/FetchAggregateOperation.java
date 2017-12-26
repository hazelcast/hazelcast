/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream.impl.aggregation;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.datastream.impl.DSDataSerializerHook;
import com.hazelcast.datastream.impl.operations.DataStreamOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class FetchAggregateOperation extends DataStreamOperation {

    private String aggregatorId;
    private transient Aggregator response;

    public FetchAggregateOperation() {
    }

    public FetchAggregateOperation(String name, String aggregatorId) {
        super(name);
        this.aggregatorId = aggregatorId;
    }

    @Override
    public void run() throws Exception {
        response = partition.fetchAggregate(aggregatorId);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getId() {
        return DSDataSerializerHook.FETCH_AGGREGATOR_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeUTF(aggregatorId);

    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        aggregatorId = in.readUTF();
    }
}
