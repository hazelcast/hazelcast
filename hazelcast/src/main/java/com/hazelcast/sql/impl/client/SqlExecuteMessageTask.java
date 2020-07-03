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
import com.hazelcast.sql.SqlQuery;
import com.hazelcast.sql.impl.SqlInternalService;
import com.hazelcast.sql.impl.SqlResultImpl;
import com.hazelcast.sql.impl.SqlServiceImpl;

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
        try {
            SqlQuery query = new SqlQuery(parameters.sql);

            if (parameters.parameters != null && !parameters.parameters.isEmpty()) {
                for (Data param : parameters.parameters) {
                    query.addParameter(serializationService.toObject(param));
                }
            }

            query.setTimeoutMillis(parameters.timeoutMillis);
            query.setCursorBufferSize(parameters.cursorBufferSize);

            SqlServiceImpl sqlService = nodeEngine.getSqlService();

            SqlResultImpl cursor = (SqlResultImpl) sqlService.query(query);

            SqlPage page = sqlService.getInternalService().getClientStateRegistry().registerAndFetch(
                endpoint.getUuid(),
                cursor,
                parameters.cursorBufferSize,
                serializationService
            );

            return new SqlExecuteResponse(
                cursor.getQueryId(),
                cursor.getRowMetadata(),
                page,
                null
            );
        } catch (Exception e) {
            SqlError error = SqlClientUtils.exceptionToClientError(e, nodeEngine.getLocalMember().getUuid());

            return new SqlExecuteResponse(null, null, null, error);
        }
    }

    @Override
    protected SqlExecuteCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return SqlExecuteCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        SqlExecuteResponse response0 = (SqlExecuteResponse) response;

        return SqlExecuteCodec.encodeResponse(
            response0.getQueryId(),
            response0.getRowMetadata(),
            response0.getPage(),
            response0.getError()
        );
    }

    @Override
    public String getServiceName() {
        return SqlInternalService.SERVICE_NAME;
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
        return new Object[] {
            parameters.sql,
            parameters.parameters,
            parameters.timeoutMillis,
            parameters.cursorBufferSize
        } ;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
