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

package com.hazelcast.client.impl.protocol.task.crdt.pncounter;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.PNCounterGetConfiguredReplicaCountCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.crdt.pncounter.PNCounterService;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.PNCounterPermission;

import java.security.Permission;

/**
 * Task responsible for processing client messages for returning the max
 * configured replica count for a {@link PNCounter}.
 */
public class PNCounterGetConfiguredReplicaCountMessageTask
        extends AbstractCallableMessageTask<String> {


    public PNCounterGetConfiguredReplicaCountMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return PNCounterGetConfiguredReplicaCountCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return PNCounterGetConfiguredReplicaCountCodec.encodeResponse((Integer) response);
    }

    @Override
    protected Object call() throws Exception {
        return nodeEngine.getConfig().findPNCounterConfig(parameters).getReplicaCount();
    }

    @Override
    public String getServiceName() {
        return PNCounterService.SERVICE_NAME;
    }

    public Object[] getParameters() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return new PNCounterPermission(parameters, ActionConstants.ACTION_READ);
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
    }
}
