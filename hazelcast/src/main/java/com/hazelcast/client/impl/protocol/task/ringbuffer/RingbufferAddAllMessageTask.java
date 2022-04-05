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
import com.hazelcast.client.impl.protocol.codec.RingbufferAddAllCodec;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.ringbuffer.impl.operations.AddAllOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.RingBufferPermission;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.List;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link com.hazelcast.client.impl.protocol.codec.RingbufferMessageType#RINGBUFFER_ADDALL}
 */
public class RingbufferAddAllMessageTask
        extends AbstractPartitionMessageTask<RingbufferAddAllCodec.RequestParameters> {

    public RingbufferAddAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new AddAllOperation(parameters.name, items(), OverflowPolicy.getById(parameters.overflowPolicy));
    }

    private Data[] items() {
        List<Data> valueList = parameters.valueList;
        Data[] array = new Data[valueList.size()];
        return valueList.toArray(array);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return RingbufferAddAllCodec.encodeResponse((Long) response);
    }

    @Override
    protected RingbufferAddAllCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return RingbufferAddAllCodec.decodeRequest(clientMessage);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.valueList, OverflowPolicy.getById(parameters.overflowPolicy)};
    }

    @Override
    public Permission getRequiredPermission() {
        return new RingBufferPermission(parameters.name, ActionConstants.ACTION_PUT);
    }

    @Override
    public String getMethodName() {
        return "addAll";
    }

    @Override
    public String getServiceName() {
        return RingbufferService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }
}
