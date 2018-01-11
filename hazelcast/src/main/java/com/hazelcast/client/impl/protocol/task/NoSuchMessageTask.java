/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;

import java.security.Permission;

public class NoSuchMessageTask extends AbstractMessageTask<ClientMessage> {

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
        String message = "Unrecognized client message received with type: 0x"
                + Integer.toHexString(parameters.getMessageType());
        logger.warning(message);
        throw new UnsupportedOperationException(message);
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

    // overriding the partition ID send from client as it is not recognized
    @Override
    public int getPartitionId() {
        return -1;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
