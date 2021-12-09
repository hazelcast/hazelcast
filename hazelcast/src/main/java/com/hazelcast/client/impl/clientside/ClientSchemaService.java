/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.spi.ClientInvocationService;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.serialization.impl.compact.SchemaService;
import com.hazelcast.logging.ILogger;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClientSchemaService implements SchemaService {

    private final Map<Long, Schema> schemas = new ConcurrentHashMap<>();
    private final ClientInvocationService invocationService;
    private final ILogger logger;

    public ClientSchemaService(ClientInvocationService invocationService, ILogger logger) {
        this.invocationService = invocationService;
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
        ClientMessage request = ClientFetchSchemaCodec.encodeRequest(schemaId);
        ClientMessage message = invocationService.invokeOnRandom(request, SERVICE_NAME).joinInternal();
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
        invocationService.invokeOnRandom(clientMessage, SERVICE_NAME).joinInternal();

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
        invocationService.invokeOnRandom(clientMessage, SERVICE_NAME).joinInternal();
    }
}
