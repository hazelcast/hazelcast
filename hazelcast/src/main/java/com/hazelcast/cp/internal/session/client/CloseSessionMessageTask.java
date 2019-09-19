/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.session.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPSessionCloseSessionCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionCloseSessionCodec.RequestParameters;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.session.operation.CloseSessionOp;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.nio.Connection;

/**
 * Client message task for {@link CloseSessionOp}
 */
public class CloseSessionMessageTask extends AbstractSessionMessageTask<RequestParameters, Object> {

    public CloseSessionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    CPGroupId getGroupId() {
        return parameters.groupId;
    }

    @Override
    RaftOp getRaftOp() {
        return new CloseSessionOp(parameters.sessionId);
    }

    @Override
    protected CPSessionCloseSessionCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CPSessionCloseSessionCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CPSessionCloseSessionCodec.encodeResponse((Boolean) response);
    }
}
