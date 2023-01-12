/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Sent from the coordinator member to participants node so that they
 * will mark the schema in their local as replicated.
 *
 * Must be sent from the coordinator only after making sure that each
 * participant member has the schema in their local with the
 * prepared/replicated status.
 */
public class AckSchemaReplicationOperation extends AbstractSchemaReplicationOperation {

    private long schemaId;

    public AckSchemaReplicationOperation() {
    }

    public AckSchemaReplicationOperation(long schemaId, int memberListVersion) {
        super(memberListVersion);
        this.schemaId = schemaId;
    }

    @Override
    protected void runInternal() {
        MemberSchemaService service = getService();
        service.onSchemaAckRequest(schemaId);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeLong(schemaId);
        out.writeInt(memberListVersion);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        schemaId = in.readLong();
        memberListVersion = in.readInt();
    }

    @Override
    public int getClassId() {
        return SchemaDataSerializerHook.ACK_SCHEMA_REPLICATION_OPERATION;
    }
}
