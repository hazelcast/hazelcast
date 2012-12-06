/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.nio.Data;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.NodeService;
import com.hazelcast.spi.Operation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @mdogan 10/1/12
 */

public class OperationWrapper extends AbstractOperation {

    private Data operationData;

    private transient Operation operation;

    public OperationWrapper() {
    }

    public OperationWrapper(final Data operation) {
        this.operationData = operation;
    }

    public OperationWrapper(final Operation operation) {
        this.operationData = IOUtil.toData(operation);
        this.operation = operation;
    }

    @Override
    public void beforeRun() throws Exception {
        final NodeService nodeService = getNodeService();
        operation = (Operation) nodeService.toObject(operationData);
        operation.setNodeService(nodeService)
                .setCaller(getCaller())
                .setCallId(getCallId())
                .setConnection(getConnection())
                .setServiceName(getServiceName())
                .setPartitionId(getPartitionId())
                .setReplicaIndex(getReplicaIndex())
                .setResponseHandler(getResponseHandler());
    }

    public void run() throws Exception {
        if (operation != null) {
            final NodeService nodeService = getNodeService();
            nodeService.runOperation(operation);
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    protected void writeInternal(final DataOutput out) throws IOException {
        operationData.writeData(out);
    }

    @Override
    protected void readInternal(final DataInput in) throws IOException {
        operationData = new Data();
        operationData.readData(in);
    }
}
