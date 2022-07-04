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
import java.util.ArrayList;
import java.util.Collection;

/**
 * Puts all the schemas to the local registry of this member if they are
 * absent.
 */
public class SendAllSchemasOperation extends Operation implements IdentifiedDataSerializable {

    private Collection<Schema> schemas;

    public SendAllSchemasOperation() {
    }

    public SendAllSchemasOperation(Collection<Schema> schemas) {
        this.schemas = schemas;
    }

    @Override
    public void run() {
        MemberSchemaService schemaService = getService();
        for (Schema schema : schemas) {
            schemaService.putIfAbsent(schema);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        int size = schemas.size();
        out.writeInt(size);
        for (Schema schema : schemas) {
            out.writeObject(schema);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        schemas = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Schema schema = in.readObject();
            schemas.add(schema);
        }
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
        return SchemaDataSerializerHook.SEND_ALL_SCHEMAS_OPERATION;
    }
}
