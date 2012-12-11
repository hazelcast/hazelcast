/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.queue;

import com.hazelcast.nio.Data;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.NodeService;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * @ali 12/6/12
 */
public class QueueBackupOperation extends AbstractOperation implements BackupOperation {


    private Data operationData;

    private transient QueueOperation operation;

    public QueueBackupOperation() {
    }

    public QueueBackupOperation(final Data operation) {
        this.operationData = operation;
    }

    public QueueBackupOperation(final QueueOperation operation) {
        this.operationData = IOUtil.toData(operation);
        this.operation = operation;
    }

    
    public void beforeRun() throws Exception {
        final NodeService nodeService = getNodeService();
        operation = (QueueOperation) nodeService.toObject(operationData);
        operation.setNodeService(nodeService)
                .setCaller(getCaller())
                .setCallId(getCallId())
                .setConnection(getConnection())
                .setServiceName(getServiceName())
                .setPartitionId(getPartitionId())
                .setReplicaIndex(getReplicaIndex())
                .setResponseHandler(getResponseHandler());
        operation.beforeRun();
    }

    public void run() throws Exception {
        operation.run();
    }

    public void afterRun() throws Exception {
        operation.afterRun();
    }

    public boolean returnsResponse() {
        return true;
    }

    public Object getResponse() {
        return true;
    }

    public boolean needsBackup() {
        return false;
    }

    protected void writeInternal(final DataOutput out) throws IOException {
        operationData.writeData(out);
    }

    protected void readInternal(final DataInput in) throws IOException {
        operationData = new Data();
        operationData.readData(in);
    }
}
