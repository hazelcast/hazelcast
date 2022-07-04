/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.flakeidgen.impl;

import com.hazelcast.flakeidgen.impl.FlakeIdGeneratorProxy.IdBatchAndWaitTime;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

class NewIdBatchOperation extends Operation implements IdentifiedDataSerializable {

    private String flakeIdGenName;
    private int batchSize;

    // for deserialization
    NewIdBatchOperation() {
    }

    NewIdBatchOperation(String genName, int batchSize) {
        this.flakeIdGenName = genName;
        this.batchSize = batchSize;
    }

    @Override
    public void run() throws Exception {
        FlakeIdGeneratorProxy proxy = (FlakeIdGeneratorProxy) getNodeEngine().getProxyService()
                .getDistributedObject(getServiceName(), flakeIdGenName, getCallerUuid());
        final IdBatchAndWaitTime result = proxy.newIdBaseLocal(batchSize);
        if (result.waitTimeMillis == 0) {
            sendResponse(result.idBatch.base());
        } else {
            getNodeEngine().getExecutionService().schedule(new Runnable() {
                @Override
                public void run() {
                    sendResponse(result.idBatch.base());
                }
            }, result.waitTimeMillis, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public String getServiceName() {
        return FlakeIdGeneratorService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return FlakeIdGeneratorDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return FlakeIdGeneratorDataSerializerHook.NEW_ID_BATCH_OPERATION;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        flakeIdGenName = in.readString();
        batchSize = in.readInt();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(flakeIdGenName);
        out.writeInt(batchSize);
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", flakeIdGenName=").append(flakeIdGenName);
        sb.append(", batchSize=").append(batchSize);
    }
}
