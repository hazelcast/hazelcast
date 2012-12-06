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

package com.hazelcast.spi;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.impl.NodeServiceImpl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class Operation implements DataSerializable {

    //serialized
    private String serviceName = null;
    private int partitionId;
    private int replicaIndex;
    private long callId = -1;
    private boolean validateTarget = true;
    // injected
    private transient NodeService nodeService = null;
    private transient Object service;
    private transient Address caller;
    private transient Connection connection;
    private transient ResponseHandler responseHandler;

    public abstract void beforeRun() throws Exception;

    public abstract void run() throws Exception;

    public abstract void afterRun() throws Exception;

    public abstract boolean returnsResponse();

    public abstract Object getResponse();

    public abstract int getSyncBackupCount();

    public abstract int getAsyncBackupCount();

    public abstract Operation getBackupOperation();

    public boolean needsBackup() {
        return getSyncBackupCount() > 0 || getAsyncBackupCount() > 0;
    }

    public String getServiceName() {
        return serviceName;
    }

    public final Operation setServiceName(String serviceName) {
        this.serviceName = serviceName;
        return this;
    }

    public final int getPartitionId() {
        return partitionId;
    }

    public final Operation setPartitionId(int partitionId) {
        this.partitionId = partitionId;
        return this;
    }

    public final int getReplicaIndex() {
        return replicaIndex;
    }

    public final Operation setReplicaIndex(int replicaIndex) {
        this.replicaIndex = replicaIndex;
        return this;
    }

    public final long getCallId() {
        return callId;
    }

    public final Operation setCallId(long callId) {
        this.callId = callId;
        return this;
    }

    public boolean validatesTarget() {
        return validateTarget;
    }

    public final Operation setValidateTarget(boolean validateTarget) {
        this.validateTarget = validateTarget;
        return this;
    }

    public final NodeService getNodeService() {
        return nodeService;
    }

    public final Operation setNodeService(NodeService nodeService) {
        this.nodeService = nodeService;
        return this;
    }

    public final <T> T getService() {
        if (service == null) {
            // one might have overridden getServiceName() method...
            final String name = serviceName != null ? serviceName : getServiceName();
            service = ((NodeServiceImpl) nodeService).getService(name);
            if (service == null) {
                throw new HazelcastException("Service with name '" + name + "' not found!");
            }
        }
        return (T) service;
    }

    public final Operation setService(Object service) {
        this.service = service;
        return this;
    }

    public final Address getCaller() {
        return caller;
    }

    public final Operation setCaller(Address caller) {
        this.caller = caller;
        return this;
    }

    public final Connection getConnection() {
        return connection;
    }

    public final Operation setConnection(Connection connection) {
        this.connection = connection;
        return this;
    }

    public final Operation setResponseHandler(ResponseHandler responseHandler) {
        this.responseHandler = responseHandler;
        return this;
    }

    public final ResponseHandler getResponseHandler() {
        return responseHandler;
    }

    public final void writeData(DataOutput out) throws IOException {
        IOUtil.writeNullableString(out, serviceName);
        out.writeInt(partitionId);
        out.writeInt(replicaIndex);
        out.writeLong(callId);
        out.writeBoolean(validateTarget);
        writeInternal(out);
    }

    public final void readData(DataInput in) throws IOException {
        serviceName = IOUtil.readNullableString(in);
        partitionId = in.readInt();
        replicaIndex = in.readInt();
        callId = in.readLong();
        validateTarget = in.readBoolean();
        readInternal(in);
    }

    protected abstract void writeInternal(DataOutput out) throws IOException;

    protected abstract void readInternal(DataInput in) throws IOException;
}
