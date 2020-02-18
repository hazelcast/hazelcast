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
import com.hazelcast.client.impl.protocol.codec.SqlFetchCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.QueryId;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * SQL query fetch task.
 */
public class SqlFetchMessageTask extends AbstractCallableMessageTask<SqlFetchCodec.RequestParameters> {
    public SqlFetchMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        QueryId queryId = serializationService.toObject(parameters.queryId);
        int pageSize = parameters.pageSize;

        return nodeEngine.getSqlService().clientFetch(endpoint.getUuid(), queryId, pageSize);
    }

    @Override
    protected SqlFetchCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return SqlFetchCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        SqlClientPage page = ((SqlClientPage) response);

        List<Data> rows;

        if (page.getRows().isEmpty()) {
            rows = Collections.emptyList();
        } else {
            rows = new ArrayList<>(page.getRows().size());

            for (SqlRow row : page.getRows()) {
                rows.add(serializationService.toData(row));
            }
        }

        return SqlFetchCodec.encodeResponse(rows, page.isLast());
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
        return "fetch";
    }

    @Override
    public Object[] getParameters() {
        // TODO: Do we need it?
        return new Object[] { parameters.queryId, parameters.pageSize } ;
    }

    @Override
    public Permission getRequiredPermission() {
        // TODO: What kind of permission is needed here?
        return null;
    }
}
