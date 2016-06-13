/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.jet;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.JetExecuteCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.jet.impl.operation.ExecutionApplicationRequestOperation;
import com.hazelcast.jet.impl.operation.JetOperation;
import com.hazelcast.nio.Connection;

public class JetExecuteMessageTask
        extends JetMessageTask<JetExecuteCodec.RequestParameters> {
    public JetExecuteMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected JetExecuteCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return JetExecuteCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return JetExecuteCodec.encodeResponse(true);
    }

    @Override
    protected JetOperation prepareOperation() {
        return new ExecutionApplicationRequestOperation(getApplicationName());
    }

    @Override
    protected String getApplicationName() {
        return this.parameters.name;
    }

    @Override
    public String getMethodName() {
        return "execute";
    }

}
