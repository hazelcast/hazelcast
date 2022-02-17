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
import com.hazelcast.client.impl.protocol.codec.CPSessionHeartbeatSessionCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionHeartbeatSessionCodec.RequestParameters;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.session.operation.HeartbeatSessionOp;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;

/**
 * Client message task for {@link HeartbeatSessionOp}
 */
public class HeartbeatSessionMessageTask extends AbstractSessionMessageTask<RequestParameters, Object> {

    public HeartbeatSessionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    CPGroupId getGroupId() {
        return parameters.groupId;
    }

    @Override
    RaftOp getRaftOp() {
        return new HeartbeatSessionOp(parameters.sessionId);
    }

    @Override
    protected CPSessionHeartbeatSessionCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CPSessionHeartbeatSessionCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CPSessionHeartbeatSessionCodec.encodeResponse();
    }
}
