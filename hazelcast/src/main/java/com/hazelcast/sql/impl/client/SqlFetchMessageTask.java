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
import com.hazelcast.client.impl.protocol.codec.SqlExecute2Codec;
import com.hazelcast.client.impl.protocol.codec.SqlFetch2Codec;
import com.hazelcast.client.impl.protocol.codec.SqlFetchCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.sql.impl.SqlInternalService;

import java.security.AccessControlException;
import java.security.Permission;
import java.util.UUID;

/**
 * SQL query fetch task.
 */
public class SqlFetchMessageTask extends SqlAbstractMessageTask<SqlFetch2Codec.RequestParameters> {

    public SqlFetchMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {

        SqlInternalService service = nodeEngine.getSqlService().getInternalService();

        return service.getClientStateRegistry().fetch(
                endpoint.getUuid(),
                parameters.queryId,
                parameters.cursorBufferSize,
                serializationService
        );
    }

    @Override
    protected SqlFetch2Codec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return SqlFetch2Codec.decodeRequest(clientMessage);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected ClientMessage encodeResponse(Object response) {
        SqlPage page = ((SqlPage) response);
        return SqlFetch2Codec.encodeResponse(page, null);
    }

    @Override
    protected ClientMessage encodeException(Throwable throwable) {
        if (throwable instanceof AccessControlException) {
            return super.encodeException(throwable);
        }
        if (!(throwable instanceof Exception)) {
            return super.encodeException(throwable);
        }
        SqlError error = SqlClientUtils.exceptionToClientError((Exception) throwable, nodeEngine.getLocalMember().getUuid());
        return SqlExecute2Codec.encodeResponse(
                null,
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
