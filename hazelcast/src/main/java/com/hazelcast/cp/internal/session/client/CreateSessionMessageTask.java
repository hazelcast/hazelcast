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

package com.hazelcast.cp.internal.session.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPSessionCreateSessionCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionCreateSessionCodec.RequestParameters;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.session.SessionResponse;
import com.hazelcast.cp.internal.session.operation.CreateSessionOp;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;

import static com.hazelcast.cp.session.CPSession.CPSessionOwnerType.CLIENT;

/**
 * Client message task for {@link CreateSessionOp}
 */
public class CreateSessionMessageTask extends AbstractSessionMessageTask<RequestParameters, SessionResponse> {

    public CreateSessionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    CPGroupId getGroupId() {
        return parameters.groupId;
    }

    @Override
    RaftOp getRaftOp() {
        return new CreateSessionOp(connection.getRemoteAddress(), parameters.endpointName, CLIENT);
    }

    @Override
    protected CPSessionCreateSessionCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CPSessionCreateSessionCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        SessionResponse session = (SessionResponse) response;
        return CPSessionCreateSessionCodec.encodeResponse(session.getSessionId(), session.getTtlMillis(),
                session.getHeartbeatMillis());
    }
}
