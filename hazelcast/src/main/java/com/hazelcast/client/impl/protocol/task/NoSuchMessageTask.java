/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddVectorCollectionConfigCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionClearCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionDeleteCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionGetCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionOptimizeCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionPutAllCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionPutCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionPutIfAbsentCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionSearchNearVectorCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionSetCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionSizeCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;

import java.security.Permission;
import java.util.Set;

import static com.hazelcast.vector.impl.spi.VectorCollectionLocator.MISSED_VECTOR_MODULE_MESSAGE;

public class NoSuchMessageTask extends AbstractMessageTask<ClientMessage> {

    private static final Set<Integer> VECTOR_MESSAGE_TASKS = Set.of(
            VectorCollectionGetCodec.REQUEST_MESSAGE_TYPE,
            VectorCollectionPutCodec.REQUEST_MESSAGE_TYPE,
            VectorCollectionPutIfAbsentCodec.REQUEST_MESSAGE_TYPE,
            VectorCollectionPutAllCodec.REQUEST_MESSAGE_TYPE,
            VectorCollectionSetCodec.REQUEST_MESSAGE_TYPE,
            VectorCollectionDeleteCodec.REQUEST_MESSAGE_TYPE,
            VectorCollectionRemoveCodec.REQUEST_MESSAGE_TYPE,
            VectorCollectionSearchNearVectorCodec.REQUEST_MESSAGE_TYPE,
            VectorCollectionOptimizeCodec.REQUEST_MESSAGE_TYPE,
            VectorCollectionClearCodec.REQUEST_MESSAGE_TYPE,
            VectorCollectionSizeCodec.REQUEST_MESSAGE_TYPE,
            DynamicConfigAddVectorCollectionConfigCodec.REQUEST_MESSAGE_TYPE
    );

    public NoSuchMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage decodeClientMessage(ClientMessage clientMessage) {
        return clientMessage;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return null;
    }

    @Override
    protected void processMessage() {
        int messageType = parameters.getMessageType();
        String message = createMessage(messageType);
        logger.finest(message);
        throw new UnsupportedOperationException(message);
    }

    @Override
    protected boolean requiresAuthentication() {
        return false;
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    private String createMessage(int messageType) {
        if (VECTOR_MESSAGE_TASKS.contains(messageType)) {
            return MISSED_VECTOR_MODULE_MESSAGE;
        }
        return "Unrecognized client message received with type: 0x" + Integer.toHexString(messageType);
    }
}
