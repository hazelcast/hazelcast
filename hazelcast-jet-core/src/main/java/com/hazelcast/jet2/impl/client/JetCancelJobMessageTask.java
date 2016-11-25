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

package com.hazelcast.jet2.impl.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.JetCancelJobCodec;
import com.hazelcast.client.impl.protocol.codec.JetCancelJobCodec.RequestParameters;
import com.hazelcast.instance.Node;
import com.hazelcast.jet2.impl.EngineContext;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.Operation;

public class JetCancelJobMessageTask extends AbstractJetMessageTask<RequestParameters> {
    protected JetCancelJobMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection, JetCancelJobCodec::decodeRequest,
                o -> JetCancelJobCodec.encodeResponse());
    }

    @Override
    protected void processMessage() {
        EngineContext engineContext = getEngineContext(parameters.engineName);
        engineContext.cancelClientInvocation(parameters.executionId);
        sendResponse(null);
    }

    @Override
    protected Operation prepareOperation() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.engineName;
    }

    @Override
    public String getMethodName() {
        return "execute";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{};
    }
}
