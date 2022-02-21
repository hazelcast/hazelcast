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
import com.hazelcast.client.impl.protocol.codec.SqlFetchCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.sql.impl.SqlInternalService;

import java.security.AccessControlException;
import java.security.Permission;

/**
 * SQL query fetch task.
 */
public class SqlFetchMessageTask extends SqlAbstractMessageTask<SqlFetchCodec.RequestParameters> {

    public SqlFetchMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() {
        SqlInternalService service = nodeEngine.getSqlService().getInternalService();

        return service.getClientStateRegistry().fetch(
            parameters.queryId,
            parameters.cursorBufferSize,
            serializationService
        );
    }

    @Override
    protected SqlFetchCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return SqlFetchCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        SqlPage page = ((SqlPage) response);

        return SqlFetchCodec.encodeResponse(page, null);
    }

    @Override
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

        return SqlFetchCodec.encodeResponse(
            null,
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
        return "fetch";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.queryId, parameters.cursorBufferSize};
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
