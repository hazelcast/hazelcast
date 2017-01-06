/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.JetCompleteResourceCodec;
import com.hazelcast.client.impl.protocol.codec.JetCompleteResourceCodec.RequestParameters;
import com.hazelcast.instance.Node;
import com.hazelcast.jet.impl.deployment.ResourceCompleteOperation;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.Operation;

public class JetCompleteResourceMessageTask extends AbstractJetMessageTask<RequestParameters> {
    protected JetCompleteResourceMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection, JetCompleteResourceCodec::decodeRequest,
                o -> JetCompleteResourceCodec.encodeResponse());
    }

    @Override
    protected Operation prepareOperation() {
        return new ResourceCompleteOperation(parameters.executionId, toObject(parameters.resourceDescriptor));
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "completeResource";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.resourceDescriptor};
    }
}
