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

package com.hazelcast.cp.internal.subsystem.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemIsEnabledCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.nio.Connection;

import java.security.Permission;

public class CPSubsystemIsEnabledMessageTask
        extends AbstractCallableMessageTask<CPSubsystemIsEnabledCodec.RequestParameters> {

    public CPSubsystemIsEnabledMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        return nodeEngine.getConfig().getCPSubsystemConfig().getCPMemberCount() > 0;
    }

    @Override
    protected CPSubsystemIsEnabledCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CPSubsystemIsEnabledCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CPSubsystemIsEnabledCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
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
        return "isEnabled";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
