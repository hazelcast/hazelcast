/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.SqlExecuteCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.QueryId;

import java.security.Permission;

/**
 * SQL query execute task.
 */
public class SqlExecuteMessageTask extends AbstractCallableMessageTask<SqlExecuteCodec.RequestParameters> {
    public SqlExecuteMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        String query = parameters.query;
        Object[] params;

        if (parameters.parameters != null && !parameters.parameters.isEmpty()) {
            params = new Object[parameters.parameters.size()];

            int idx = 0;

            for (Data param : parameters.parameters) {
                 params[idx++] = serializationService.toObject(param);
            }
        } else {
            params = null;
        }

        QueryId queryId = nodeEngine.getSqlService().clientQuery(endpoint.getUuid(), query, params);

        return serializationService.toData(queryId);
    }

    @Override
    protected SqlExecuteCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return SqlExecuteCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return SqlExecuteCodec.encodeResponse((Data) response);
    }

    @Override
    public String getServiceName() {
        return SqlService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "execute";
    }

    @Override
    public Object[] getParameters() {
        return new Object[] { parameters.query, parameters.parameters } ;
    }

    @Override
    public Permission getRequiredPermission() {
        // TODO: Permission to read queried maps.
        return null;
    }
}
