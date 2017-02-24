/*
 * Copyright (c) 2008, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.config.MapConfig;
import com.hazelcast.executor.Offloadable;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.projection.Projection;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;

import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;

public final class GetWithProjectionOperation extends GetOperation {

    private Data projection;
    private Data result;
    private String offloadableExecutorName;

    public GetWithProjectionOperation() {
    }

    public GetWithProjectionOperation(String name, Data dataKey, Data projection) {
        super(name, dataKey);

        this.dataKey = dataKey;
        this.projection = projection;
    }

    @Override
    public void run() {
        Object value = recordStore.get(dataKey, false);

        SerializationService ss = getNodeEngine().getSerializationService();
        Projection projObject = ss.toObject(projection);
        offloadableExecutorName = null;
        if (projObject instanceof Offloadable) {
            offloadableExecutorName = ((Offloadable) projObject).getExecutorName();
        }

        if (offloadableExecutorName != null) {
            // off-load the projection code to the specified executor
            value = isObjectFormat() ? ss.toData(value) : value;
            runOffloadedProjection(value, offloadableExecutorName, projObject, (OperationServiceImpl)
                    getNodeEngine().getOperationService(), ss);
        } else {
            value = isObjectFormat() ? value : ss.toObject(value);
            result = mapServiceContext.toData(projObject.transform(value));
        }
    }

    private void runOffloadedProjection(final Object value, String executorName, final Projection proj,
                                        final OperationServiceImpl ops, final SerializationService ss) {
        ops.onStartAsyncOperation(this);
        executorName = executorName.equals("") ? ASYNC_EXECUTOR : executorName;
        getNodeEngine().getExecutionService().execute(executorName, new Runnable() {
            @Override
            public void run() {
                Object result;
                try {
                    result = mapServiceContext.toData(proj.transform(ss.toObject(value)));
                    getOperationResponseHandler().sendResponse(GetWithProjectionOperation.this, result);
                } catch (Exception e) {
                    getOperationResponseHandler().sendResponse(GetWithProjectionOperation.this, e);
                } finally {
                    ops.onCompletionAsyncOperation(GetWithProjectionOperation.this);
                }
            }
        });
    }

    private boolean isObjectFormat() {
        MapConfig mapConfig = getNodeEngine().getConfig().getMapConfig(name);
        return OBJECT.equals(mapConfig.getInMemoryFormat());
    }

    @Override
    public Data getResponse() {
        return result;
    }

    @Override
    public boolean returnsResponse() {
        return offloadableExecutorName == null;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.GET_WITH_PROJECTION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(projection);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        projection = in.readData();
    }
}
