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
import com.hazelcast.client.impl.protocol.codec.SqlCloseCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlInternalService;

import java.security.Permission;

/**
 * SQL query close task.
 */
public class SqlCloseMessageTask extends SqlAbstractMessageTask<QueryId> {
    public SqlCloseMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() {
        SqlInternalService service = nodeEngine.getSqlService().getInternalService();

        service.getClientStateRegistry().close(endpoint.getUuid(), parameters);

        return null;
    }

    @Override
    protected QueryId decodeClientMessage(ClientMessage clientMessage) {
        return SqlCloseCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return SqlCloseCodec.encodeResponse();
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
        return "close";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters};
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
