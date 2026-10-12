/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPMapGetCodec;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapGetOp;
import com.hazelcast.cp.internal.raft.QueryPolicy;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.security.SecurityInterceptorConstants;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.CPMapPermission;

import java.security.Permission;

public class GetMessageTask extends CPMapAbstractMessageTask<CPMapGetCodec.RequestParameters> {
    public GetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    public Permission getRequiredPermission() {
        return new CPMapPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    protected CPMapGetCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CPMapGetCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        Data dataResponse = serializationService.toData(response);
        return CPMapGetCodec.encodeResponse(dataResponse);
    }

    @Override
    protected void processMessage() throws Throwable {
        CPMapGetOp op = new CPMapGetOp(parameters.name, parameters.key);
        query(parameters.groupId, op, QueryPolicy.LINEARIZABLE);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.GET;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.key};
    }
}
