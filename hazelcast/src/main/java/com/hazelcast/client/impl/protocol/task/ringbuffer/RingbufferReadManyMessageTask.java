/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.RingbufferReadManyCodec;
import com.hazelcast.core.IFunction;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;
import com.hazelcast.ringbuffer.impl.operations.ReadManyOperation;
import com.hazelcast.security.SecurityInterceptorConstants;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.RingBufferPermission;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.ArrayList;
import java.util.List;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link com.hazelcast.client.impl.protocol.codec.RingbufferReadManyCodec#REQUEST_MESSAGE_TYPE}
 */
public class RingbufferReadManyMessageTask
        extends AbstractRingbufferMessageTask<RingbufferReadManyCodec.RequestParameters> {

    public RingbufferReadManyMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
        setNamespaceAware();
    }

    @Override
    protected Operation prepareOperation() {
        IFunction<?, Boolean> function = serializationService.toObject(parameters.filter);
        return new ReadManyOperation(
                parameters.name,
                parameters.startSequence,
                parameters.minCount,
                parameters.maxCount,
                function);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        // we are not deserializing the whole content, only the enclosing portable. The actual items remain un
        final ReadResultSetImpl resultSet = nodeEngine.getSerializationService().toObject(response);
        final List<Data> items = new ArrayList<>(resultSet.size());
        final long[] seqs = new long[resultSet.size()];
        final Data[] dataItems = resultSet.getDataItems();

        for (int k = 0; k < resultSet.size(); k++) {
            items.add(dataItems[k]);
            seqs[k] = resultSet.getSequence(k);
        }

        return RingbufferReadManyCodec.encodeResponse(resultSet.readCount(), items, seqs, resultSet.getNextSequenceToReadFrom());
    }

    @Override
    protected RingbufferReadManyCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return RingbufferReadManyCodec.decodeRequest(clientMessage);
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.READ_MANY;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Permission getRequiredPermission() {
        return new RingBufferPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public Object[] getParameters() {

        return new Object[]{parameters.startSequence, parameters.minCount, parameters.maxCount, null};
    }
}
