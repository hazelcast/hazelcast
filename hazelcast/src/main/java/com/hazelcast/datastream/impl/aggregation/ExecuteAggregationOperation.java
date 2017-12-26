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
import com.hazelcast.spi.CallStatus;
import com.hazelcast.spi.Offload;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.spi.CallStatus.DONE_RESPONSE;

public class ExecuteAggregationOperation extends DataStreamOperation {

    private Map<String, Object> bindings;
    private String preparationId;
    private boolean forkJoin;
    private transient Aggregator response;

    public ExecuteAggregationOperation() {
    }

    public ExecuteAggregationOperation(String name,
                                       String preparationId,
                                       Map<String, Object> bindings,
                                       boolean forkJoin) {
        super(name);
        this.preparationId = preparationId;
        this.bindings = bindings;
        this.forkJoin = forkJoin;
    }

    @Override
    public CallStatus call() throws Exception {
        if (forkJoin) {
            return new Offload(this) {
                @Override
                public void start() throws Exception {
                    OperationServiceImpl ops = (OperationServiceImpl) getNodeEngine().getOperationService();
                    ops.onStartAsyncOperation(ExecuteAggregationOperation.this);

                    // bit hackish (probably better to have a single future returned by combining the results)
                    AggregateFJResult result = partition.executeAggregateFJ(preparationId, bindings);
                    result.future.thenAccept(aggregator -> {
                        try {
                            result.aggregator.combine(aggregator);
                            Aggregator response = result.aggregator;
                            sendResponse(response);
                        } finally {
                            ops.onCompletionAsyncOperation(ExecuteAggregationOperation.this);
                        }
                    });
                }
            };
        } else {
            response = partition.executeAggregationPartitionThread(preparationId, bindings);
            return DONE_RESPONSE;
        }
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getId() {
        return DSDataSerializerHook.EXECUTE_AGGREGATION_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(preparationId);
        out.writeBoolean(forkJoin);
        out.writeInt(bindings.size());
        for (Map.Entry<String, Object> entry : bindings.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        preparationId = in.readUTF();
        forkJoin = in.readBoolean();
        int size = in.readInt();
        bindings = new HashMap<>(size);
        for (int k = 0; k < size; k++) {
            bindings.put(in.readUTF(), in.readObject());
        }
    }
}
