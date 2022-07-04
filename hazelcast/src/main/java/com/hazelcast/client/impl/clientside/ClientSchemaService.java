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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientFetchSchemaCodec;
import com.hazelcast.client.impl.protocol.codec.ClientSendAllSchemasCodec;
import com.hazelcast.client.impl.protocol.codec.ClientSendSchemaCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.serialization.impl.compact.SchemaService;
import com.hazelcast.logging.ILogger;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClientSchemaService implements SchemaService {

    private final Map<Long, Schema> schemas = new ConcurrentHashMap<>();
    private final HazelcastClientInstanceImpl client;
    private final ILogger logger;

    public ClientSchemaService(HazelcastClientInstanceImpl client, ILogger logger) {
        this.client = client;
        this.logger = logger;
    }

    @Override
    public Schema get(long schemaId) {
        Schema schema = schemas.get(schemaId);
        if (schema != null) {
            return schema;
        }
        if (logger.isFinestEnabled()) {
            logger.finest("Could not find schema id  " + schemaId + " locally, will search on the cluster" + schemaId);
        }
        ClientInvocation invocation = new ClientInvocation(client, ClientFetchSchemaCodec.encodeRequest(schemaId), SERVICE_NAME);
        ClientMessage message = invocation.invoke().joinInternal();
        schema = ClientFetchSchemaCodec.decodeResponse(message);
        if (schema != null) {
            schemas.put(schemaId, schema);
        }
        return schema;
    }

    @Override
    public void put(Schema schema) {
        long schemaId = schema.getSchemaId();
        Schema existingSchema = schemas.get(schemaId);
        if (existingSchema != null) {
            return;
        }

        ClientMessage clientMessage = ClientSendSchemaCodec.encodeRequest(schema);
        ClientInvocation invocation = new ClientInvocation(client, clientMessage, SERVICE_NAME);
        invocation.invoke().joinInternal();

        putIfAbsent(schema);
    }

    @Override
    public void putLocal(Schema schema) {
        putIfAbsent(schema);
    }

    private boolean putIfAbsent(Schema schema) {
        long schemaId = schema.getSchemaId();
        Schema existingSchema = schemas.putIfAbsent(schemaId, schema);
        if (existingSchema == null) {
            return true;
        }
        if (!schema.equals(existingSchema)) {
            throw new IllegalStateException("Schema with schemaId " + schemaId + " already exists. "
                    + "existing schema " + existingSchema
                    + "new schema " + schema);
        }
        return false;
    }

    public void sendAllSchemas() {
        if (schemas.isEmpty()) {
            if (logger.isFinestEnabled()) {
                logger.finest("There is no schema to send to the cluster");
            }
            return;
        }
        if (logger.isFinestEnabled()) {
            logger.finest("Sending schemas to the cluster " + schemas);
        }
        ClientMessage clientMessage = ClientSendAllSchemasCodec.encodeRequest(new ArrayList<>(schemas.values()));
        ClientInvocation invocation = new ClientInvocation(client, clientMessage, SERVICE_NAME);
        invocation.invokeUrgent().joinInternal();
    }
}
