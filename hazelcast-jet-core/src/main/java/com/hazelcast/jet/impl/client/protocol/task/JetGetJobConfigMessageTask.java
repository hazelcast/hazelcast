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

package com.hazelcast.jet.impl.client.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.BlockingMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.impl.client.protocol.codec.JetGetJobConfigCodec;
import com.hazelcast.jet.impl.operation.GetJobConfigOperation;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;

public class JetGetJobConfigMessageTask extends AbstractJetMessageTask<JetGetJobConfigCodec.RequestParameters, Data>
        implements BlockingMessageTask {

    protected JetGetJobConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection,
                JetGetJobConfigCodec::decodeRequest,
                JetGetJobConfigCodec::encodeResponse);
    }

    @Override
    protected Operation prepareOperation() {
        return new GetJobConfigOperation(parameters.jobId);
    }


    @Override
    protected Object processResponseBeforeSending(Object response) {
        SerializationService serializationService = nodeEngine.getSerializationService();
        return serializationService.toData(response);
    }

    @Override
    public String getMethodName() {
        return "getJobConfig";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

}
