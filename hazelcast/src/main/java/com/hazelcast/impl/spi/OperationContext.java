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

public class OperationContext {
    NodeService nodeService;
    Object service = null;
    ResponseHandler responseHandler = null;
    Address caller = null;
    long callId = -1;
    boolean local = true;
    int partitionId;

    public NodeService getNodeService() {
        return nodeService;
    }

    public OperationContext setNodeService(NodeService nodeService) {
        this.nodeService = nodeService;
        return OperationContext.this;
    }

    public Object getService() {
        return service;
    }

    public OperationContext setService(Object service) {
        this.service = service;
        return OperationContext.this;
    }

    public ResponseHandler getResponseHandler() {
        return responseHandler;
    }

    public void setResponseHandler(ResponseHandler responseHandler) {
        this.responseHandler = responseHandler;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public OperationContext setPartitionId(int partitionId) {
        this.partitionId = partitionId;
        return OperationContext.this;
    }

    public Address getCaller() {
        return caller;
    }

    public OperationContext setCaller(Address caller) {
        this.caller = caller;
        return OperationContext.this;
    }

    public long getCallId() {
        return callId;
    }

    public OperationContext setCallId(long callId) {
        this.callId = callId;
        return OperationContext.this;
    }

    public boolean isLocal() {
        return local;
    }

    public OperationContext setLocal(boolean local) {
        this.local = local;
        return OperationContext.this;
    }
}
