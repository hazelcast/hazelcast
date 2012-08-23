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

package com.hazelcast.impl.spi;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.IOUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class Operation implements Runnable, DataSerializable {

    //serialized
    private String serviceName = null;
    private int partitionId;
    private int replicaIndex;
    private long callId;
    private boolean noReply = false;
    private boolean validateTarget = true;
    // injected
    private NodeService nodeService = null;
    private Object service;
    private Address caller;
    private Connection connection;
    private ResponseHandler responseHandler;

    final public void writeData(DataOutput out) throws IOException {
        IOUtil.writeNullableString(out, serviceName);
        out.writeInt(partitionId);
        out.writeInt(replicaIndex);
        out.writeLong(callId);
        out.writeBoolean(noReply);
        out.writeBoolean(validateTarget);
        writeInternal(out);
    }

    final public void readData(DataInput in) throws IOException {
        serviceName = IOUtil.readNullableString(in);
        partitionId = in.readInt();
        replicaIndex = in.readInt();
        callId = in.readLong();
        noReply = in.readBoolean();
        validateTarget = in.readBoolean();
        readInternal(in);
    }

    protected abstract void writeInternal(DataOutput out) throws IOException;

    protected abstract void readInternal(DataInput in) throws IOException;

    public String getServiceName() {
        return serviceName;
    }

    public Operation setServiceName(String serviceName) {
        this.serviceName = serviceName;
        return this;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public Operation setPartitionId(int partitionId) {
        this.partitionId = partitionId;
        return this;
    }

    public int getReplicaIndex() {
        return replicaIndex;
    }

    public Operation setReplicaIndex(int replicaIndex) {
        this.replicaIndex = replicaIndex;
        return this;
    }

    public long getCallId() {
        return callId;
    }

    public Operation setCallId(long callId) {
        this.callId = callId;
        return this;
    }

    public boolean isNoReply() {
        return noReply;
    }

    public Operation setNoReply(boolean noReply) {
        this.noReply = noReply;
        return this;
    }

    public boolean isTargetValid() {
        return validateTarget;
    }

    public Operation setValidateTarget(boolean validateTarget) {
        this.validateTarget = validateTarget;
        return this;
    }

    public NodeService getNodeService() {
        return nodeService;
    }

    public Operation setNodeService(NodeService nodeService) {
        this.nodeService = nodeService;
        return this;
    }

    public Object getService() {
        if (service == null) {
            service = nodeService.getService(serviceName);
            if (service == null) {
                throw new RuntimeException(serviceName + "  is nullTLLLLLLLllllll " + nodeService);
            }
        }
        return service;
    }

    public Operation setService(Object service) {
        this.service = service;
        return this;
    }

    public Address getCaller() {
        return caller;
    }

    public Operation setCaller(Address caller) {
        this.caller = caller;
        return this;
    }

    public Connection getConnection() {
        return connection;
    }

    public Operation setConnection(Connection connection) {
        this.connection = connection;
        return this;
    }

    public Operation setResponseHandler(ResponseHandler responseHandler) {
        this.responseHandler = responseHandler;
        return this;
    }

    public ResponseHandler getResponseHandler() {
        return responseHandler;
    }
}
