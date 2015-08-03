/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.RingbufferReadManyAsyncCodec;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.core.IFunction;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.ringbuffer.impl.client.PortableReadResultSet;
import com.hazelcast.ringbuffer.impl.operations.ReadManyOperation;
import com.hazelcast.spi.Operation;

import java.security.Permission;

/**
 * Client Protocol Task for handling messages with type id:
 * {@link com.hazelcast.client.impl.protocol.codec.QueueMessageType#QUEUE_OFFER}
 */
public class RingbufferReadManyAsyncMessageTask
        extends AbstractPartitionMessageTask<RingbufferReadManyAsyncCodec.RequestParameters> {

    public RingbufferReadManyAsyncMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        IFunction<?, Boolean> function = serializationService.toObject(parameters.filter);
        return new ReadManyOperation(
                parameters.name,
                parameters.startSequence,
                parameters.minCount,
                parameters.maxCount,
                function,
                true);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        // we are not deserializing the whole content, only the enclosing portable. The actual items remain un
        PortableReadResultSet resultSet = nodeEngine.getSerializationService().toObject(response);
        return RingbufferReadManyAsyncCodec.encodeResponse(resultSet.readCount(), resultSet.getDataItems());
    }

    @Override
    protected RingbufferReadManyAsyncCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return RingbufferReadManyAsyncCodec.decodeRequest(clientMessage);
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "readManyAsync";
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
