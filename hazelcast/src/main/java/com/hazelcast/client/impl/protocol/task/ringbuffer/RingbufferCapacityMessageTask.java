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

package com.hazelcast.client.impl.protocol.task.ringbuffer;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.RingbufferCapacityCodec;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.ringbuffer.impl.operations.GenericOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.RingBufferPermission;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;

public class RingbufferCapacityMessageTask
        extends AbstractPartitionMessageTask<String> {

    public RingbufferCapacityMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new GenericOperation(parameters, GenericOperation.OPERATION_CAPACITY);
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return RingbufferCapacityCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return RingbufferCapacityCodec.encodeResponse((Long) response);
    }

    @Override
    public String getServiceName() {
        return RingbufferService.SERVICE_NAME;
    }

    public Object[] getParameters() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return new RingBufferPermission(parameters, ActionConstants.ACTION_READ);
    }

    @Override
    public String getMethodName() {
        return "capacity";
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
    }

}
