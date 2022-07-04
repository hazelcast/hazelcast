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

package com.hazelcast.sql.impl.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.SqlExecuteCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.sql.SqlExpectedResultType;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.AbstractSqlResult;
import com.hazelcast.sql.impl.SqlInternalService;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.security.NoOpSqlSecurityContext;
import com.hazelcast.sql.impl.security.SqlSecurityContext;

import java.security.AccessControlException;
import java.security.Permission;

/**
 * SQL query execute task.
 */
public class SqlExecuteMessageTask extends SqlAbstractMessageTask<SqlExecuteCodec.RequestParameters> {
    public SqlExecuteMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() {
        SqlSecurityContext sqlSecurityContext = prepareSecurityContext();

        SqlStatement query = new SqlStatement(parameters.sql);

        for (Data param : parameters.parameters) {
            query.addParameter(serializationService.toObject(param));
        }

        query.setSchema(parameters.schema);
        query.setTimeoutMillis(parameters.timeoutMillis);
        query.setCursorBufferSize(parameters.cursorBufferSize);
        query.setExpectedResultType(SqlExpectedResultType.fromId(parameters.expectedResultType));

        SqlServiceImpl sqlService = nodeEngine.getSqlService();

        boolean skipUpdateStatistics = parameters.isSkipUpdateStatisticsExists && parameters.skipUpdateStatistics;
        return sqlService.execute(query, sqlSecurityContext, parameters.queryId, skipUpdateStatistics);
    }

    @Override
    protected SqlExecuteCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return SqlExecuteCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        AbstractSqlResult result = (AbstractSqlResult) response;

        if (result.updateCount() >= 0) {
            return SqlExecuteCodec.encodeResponse(null, null, result.updateCount(), null);
        } else {
            SqlServiceImpl sqlService = nodeEngine.getSqlService();

            SqlPage page = sqlService.getInternalService().getClientStateRegistry().registerAndFetch(
                    endpoint.getUuid(),
                    result,
                    parameters.cursorBufferSize,
                    serializationService
            );

            return SqlExecuteCodec.encodeResponse(
                    result.getRowMetadata().getColumns(),
                    page,
                    -1,
                    null
            );
        }
    }

    protected ClientMessage encodeException(Throwable throwable) {
        // exception can be thrown before parameters are decoded
        if (parameters == null) {
            return super.encodeException(throwable);
        }

        nodeEngine.getSqlService().getInternalService().getClientStateRegistry().closeOnError(parameters.queryId);

        if (throwable instanceof AccessControlException) {
            return super.encodeException(throwable);
        }
        if (!(throwable instanceof Exception)) {
            return super.encodeException(throwable);
        }
        SqlError error = SqlClientUtils.exceptionToClientError((Exception) throwable, nodeEngine.getLocalMember().getUuid());

        return SqlExecuteCodec.encodeResponse(
                null,
                null,
                -1,
                error
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
        return new Object[]{
                parameters.sql,
                parameters.parameters,
                parameters.timeoutMillis,
                parameters.cursorBufferSize,
                parameters.schema,
                parameters.queryId
        };
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    private SqlSecurityContext prepareSecurityContext() {
        SecurityContext securityContext = clientEngine.getSecurityContext();

        if (securityContext == null) {
            return NoOpSqlSecurityContext.INSTANCE;
        } else {
            return securityContext.createSqlContext(endpoint.getSubject());
        }
    }
}
