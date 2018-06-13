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

package com.hazelcast.jet.impl.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.JetReadMetricsCodec;
import com.hazelcast.client.impl.protocol.codec.JetReadMetricsCodec.RequestParameters;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.metrics.JetMetricsService;
import com.hazelcast.nio.Connection;

import java.security.Permission;

public class JetReadMetricsMessageTask extends AbstractMessageTask<RequestParameters> {

    protected JetReadMetricsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        // readMetrics requests are sent to member identified by address, but we want it by member UUID.
        // After a member restart, the address remains, but UUID changes. If the local member has different
        // UUID than then intended one, fail.
        if (!parameters.uuid.equals(nodeEngine.getLocalMember().getUuid())) {
            // do not throw RetryableException here
            throw new IllegalArgumentException(
                    "Requested metrics for member " + parameters.uuid
                            + ", but local member is " + nodeEngine.getLocalMember().getUuid()
            );
        }
        JetMetricsService jetMetricsService = getService(JetMetricsService.SERVICE_NAME);
        sendResponse(jetMetricsService.readMetrics(parameters.fromSequence));
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return JetReadMetricsCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return JetReadMetricsCodec.encodeResponse(serializationService.toData(response));
    }

    @Override
    public String getServiceName() {
        return JetService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "readMetrics";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}

