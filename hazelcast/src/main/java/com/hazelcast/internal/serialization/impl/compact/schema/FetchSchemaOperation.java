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

package com.hazelcast.internal.serialization.impl.compact.schema;

import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.serialization.impl.compact.SchemaService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

/**
 * Tries to locate the schema from its schema id in the local registry of the
 * member and return it. It is allowed to return {@code null} as the response,
 * if there is no such schema in the registry.
 */
public class FetchSchemaOperation extends Operation implements IdentifiedDataSerializable {

    private long schemaId;
    private Schema response;

    public FetchSchemaOperation() {
    }

    public FetchSchemaOperation(long schemaId) {
        this.schemaId = schemaId;
    }

    @Override
    public void run() {
        MemberSchemaService schemaService = getService();
        response = schemaService.getLocal(schemaId);
    }

    @Override
    public Schema getResponse() {
        return response;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeLong(schemaId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        schemaId = in.readLong();
    }

    @Override
    public String getServiceName() {
        return SchemaService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return SchemaDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SchemaDataSerializerHook.FETCH_SCHEMA_OPERATION;
    }
}
