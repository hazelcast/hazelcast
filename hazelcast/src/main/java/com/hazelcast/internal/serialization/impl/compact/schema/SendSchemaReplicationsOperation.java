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

import com.hazelcast.internal.serialization.impl.compact.SchemaService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Sent by the master member to the joining member to replay
 * replications occurred so far.
 *
 * The joining member replays the preparation phase for the replications
 * and puts the schema replications to its local registry with the same
 * status.
 *
 * For already {@link SchemaReplicationStatus#REPLICATED} replications,
 * replaying the preparation phase and then putting the replication with
 * the same status to the local registry is enough to maintain the same
 * status across the cluster.
 *
 * For replications in the {@link SchemaReplicationStatus#PREPARED} status,
 * the joining member will eagerly replay the preparation phases again.
 * When the coordinator members start sending {@link AckSchemaReplicationOperation}
 * to participant members for the {@link SchemaReplicationStatus#PREPARED}
 * schemas, the member list version check in the aforementioned operation
 * will make sure that the coordinator members will retry the acknowledgment
 * operation until the new member becomes one of the participants.
 */
public class SendSchemaReplicationsOperation extends Operation implements IdentifiedDataSerializable, AllowedDuringPassiveState {

    private Collection<SchemaReplication> replications;

    public SendSchemaReplicationsOperation() {
    }

    public SendSchemaReplicationsOperation(Collection<SchemaReplication> replications) {
        this.replications = replications;
    }

    @Override
    public void run() {
        MemberSchemaService schemaService = getService();
        schemaService.replayReplications(replications);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        int size = replications.size();
        out.writeInt(size);
        for (SchemaReplication replication : replications) {
            out.writeObject(replication);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        replications = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            SchemaReplication replication = in.readObject();
            replications.add(replication);
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
        return SchemaDataSerializerHook.SEND_SCHEMA_REPLICATIONS_OPERATION;
    }
}
