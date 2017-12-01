/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.flakeidgen;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;

import java.io.IOException;

class NewIdBatchOperation extends Operation implements IdentifiedDataSerializable {

    private String flakeIdGenName;
    private int batchSize;
    private long returnValue;

    // for deserialization
    NewIdBatchOperation() {
    }

    public NewIdBatchOperation(String flakeIdGenName, int batchSize) {
        this.flakeIdGenName = flakeIdGenName;
        this.batchSize = batchSize;
    }

    @Override
    public void run() throws Exception {
        FlakeIdGeneratorProxy proxy = (FlakeIdGeneratorProxy) getNodeEngine().getProxyService()
                .getDistributedObject(getServiceName(), flakeIdGenName);

        returnValue = proxy.newIdBaseLocal(batchSize);
    }

    @Override
    public Object getResponse() {
        return returnValue;
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
    public int getId() {
        return FlakeIdGeneratorDataSerializerHook.NEW_ID_BATCH_OPERATION;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        flakeIdGenName = in.readUTF();
        batchSize = in.readInt();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(flakeIdGenName);
        out.writeInt(batchSize);
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", flakeIdGenName=").append(flakeIdGenName);
        sb.append(", batchSize=").append(batchSize);
    }
}
