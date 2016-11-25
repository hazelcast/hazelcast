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
import com.hazelcast.client.impl.protocol.codec.JetExecuteJobCodec;
import com.hazelcast.client.impl.protocol.codec.JetExecuteJobCodec.RequestParameters;
import com.hazelcast.instance.Node;
import com.hazelcast.jet2.impl.EngineContext;
import com.hazelcast.jet2.impl.ExecuteJobOperation;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;

public class JetExecuteJobMessageTask extends AbstractJetMessageTask<RequestParameters> {
    protected JetExecuteJobMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection, JetExecuteJobCodec::decodeRequest,
                o -> JetExecuteJobCodec.encodeResponse());
    }

    @Override
    protected Operation prepareOperation() {
        return new ExecuteJobOperation(parameters.engineName, parameters.executionId, toObject(parameters.dag));
    }

    @Override
    protected void processMessage() {
        Operation op = this.prepareOperation();
        op.setCallerUuid(getEndpoint().getUuid());
        InvocationBuilder builder = this.getInvocationBuilder(op).setResultDeserialized(false);
        EngineContext engineContext = getEngineContext(parameters.engineName);

        InternalCompletableFuture<Object> invocation = builder.invoke();
        engineContext.registerClientInvocation(parameters.executionId, invocation);
        invocation.andThen(this);
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
        return new Object[]{parameters.dag};
    }

    @Override
    public void onResponse(Object response) {
        super.onResponse(response);
    }

    @Override
    public void onFailure(Throwable t) {
        super.onFailure(t);
    }
}
