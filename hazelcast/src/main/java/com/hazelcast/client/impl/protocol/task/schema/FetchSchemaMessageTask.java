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

package com.hazelcast.client.impl.protocol.task.schema;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientFetchSchemaCodec;
import com.hazelcast.client.impl.protocol.task.AbstractAsyncMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.serialization.impl.compact.SchemaService;
import com.hazelcast.internal.serialization.impl.compact.schema.MemberSchemaService;

import java.security.Permission;
import java.util.concurrent.CompletableFuture;

public class FetchSchemaMessageTask extends AbstractAsyncMessageTask<Long, Schema> {

    public FetchSchemaMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<Schema> processInternal() {
        MemberSchemaService memberSchemaService = getService(getServiceName());
        return memberSchemaService.getAsync(parameters);
    }

    @Override
    protected Long decodeClientMessage(ClientMessage clientMessage) {
        return ClientFetchSchemaCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientFetchSchemaCodec.encodeResponse((Schema) response);
    }

    @Override
    public String getServiceName() {
        return SchemaService.SERVICE_NAME;
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
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
