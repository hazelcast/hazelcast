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
import com.hazelcast.spi.NodeService;
import com.hazelcast.spi.Operation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;

/**
 * @mdogan 10/1/12
 */

class OperationWrapper extends Operation {

    private Data operationData;

    public OperationWrapper() {
    }

    public OperationWrapper(final Data operationData) {
        this.operationData = operationData;
    }

    public void run() {
        NodeService nodeService = getNodeService();
        try {
            Operation operation = (Operation) nodeService.toObject(operationData);
            operation.setNodeService(nodeService)
                    .setCaller(getCaller())
                    .setPartitionId(getPartitionId())
                    .setReplicaIndex(getReplicaIndex())
                    .setResponseHandler(getResponseHandler())
                    .setService(getService());
            operation.run();
        } catch (Exception e) {
            nodeService.getLogger(OperationWrapper.class.getName()).log(Level.WARNING, e.getMessage(), e);
            getResponseHandler().sendResponse(e);
        }
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
