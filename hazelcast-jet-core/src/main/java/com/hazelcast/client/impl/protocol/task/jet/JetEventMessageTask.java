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
import com.hazelcast.client.impl.protocol.codec.JetEventCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.jet.impl.operation.ApplicationEventOperation;
import com.hazelcast.jet.impl.operation.JetOperation;
import com.hazelcast.jet.impl.statemachine.application.ApplicationEvent;
import com.hazelcast.nio.Connection;

public class JetEventMessageTask extends JetMessageTask<JetEventCodec.RequestParameters> {
    public JetEventMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected String getApplicationName() {
        return this.parameters.name;
    }

    @Override
    protected JetEventCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return JetEventCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return JetEventCodec.encodeResponse(true);
    }

    @Override
    protected JetOperation prepareOperation() {
        ApplicationEvent applicationEvent = this.serializationService.toObject(this.parameters.event);
        ApplicationEventOperation operation = new ApplicationEventOperation(
                getApplicationName(), applicationEvent
        );
        return operation;
    }

    @Override
    public String getMethodName() {
        return "event";
    }

}
