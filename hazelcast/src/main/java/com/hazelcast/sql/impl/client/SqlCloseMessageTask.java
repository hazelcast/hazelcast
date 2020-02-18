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
import com.hazelcast.client.impl.protocol.codec.SqlCloseCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.QueryId;

import java.security.Permission;

/**
 * SQL query close task.
 */
public class SqlCloseMessageTask extends AbstractCallableMessageTask<SqlCloseCodec.RequestParameters> {
    public SqlCloseMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        QueryId queryId = serializationService.toObject(parameters.queryId);

        nodeEngine.getSqlService().clientClose(endpoint.getUuid(), queryId);

        return null;
    }

    @Override
    protected SqlCloseCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return SqlCloseCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return SqlCloseCodec.encodeResponse();
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
        return "close";
    }

    @Override
    public Object[] getParameters() {
        // TODO: Do we need it?
        return new Object[] { parameters.queryId } ;
    }

    @Override
    public Permission getRequiredPermission() {
        // TODO: What kind of permission is needed here?
        return null;
    }
}
