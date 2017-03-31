/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition;

import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ReplicaFragmentNamespace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

/**
 * Contains fragment namespaces along with their partition versions and migration data operations
 */
public class ReplicaFragmentMigrationState implements IdentifiedDataSerializable {

    private Map<ReplicaFragmentNamespace, long[]> namespaces;

    private Collection<Operation> migrationOperations;

    public ReplicaFragmentMigrationState() {
    }

    private ReplicaFragmentMigrationState(Map<ReplicaFragmentNamespace, long[]> namespaces,
                                          Collection<Operation> migrationOperations) {
        this.namespaces = namespaces;
        this.migrationOperations = migrationOperations;
    }

    public static ReplicaFragmentMigrationState newDefaultReplicaFragmentMigrationState(long[] replicaVersions,
                                                                                        Collection<Operation> operations) {
        Map<ReplicaFragmentNamespace, long[]> namespaces = singletonMap(InternalReplicaFragmentNamespace.INSTANCE,
                replicaVersions);
        return new ReplicaFragmentMigrationState(namespaces, operations);
    }

    public static ReplicaFragmentMigrationState newGroupedReplicaFragmentMigrationState(
            Map<ReplicaFragmentNamespace, long[]> namespaces, Operation operation) {
        return new ReplicaFragmentMigrationState(namespaces, singletonList(operation));
    }

    public Map<ReplicaFragmentNamespace, long[]> getNamespaces() {
        return namespaces;
    }

    public Collection<Operation> getMigrationOperations() {
        return migrationOperations;
    }

    @Override
    public int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.REPLICA_FRAGMENT_MIGRATION_STATE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(namespaces.size());
        for (Map.Entry<ReplicaFragmentNamespace, long[]> e : namespaces.entrySet()) {
            out.writeObject(e.getKey());
            out.writeLongArray(e.getValue());
        }
        out.writeInt(migrationOperations.size());
        for (Operation operation : migrationOperations) {
            out.writeObject(operation);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        namespaces = new HashMap<ReplicaFragmentNamespace, long[]>();
        int namespaceSize = in.readInt();
        for (int i = 0; i < namespaceSize; i++) {
            ReplicaFragmentNamespace namespace = in.readObject();
            long[] replicaVersions = in.readLongArray();
            namespaces.put(namespace, replicaVersions);
        }
        migrationOperations = new ArrayList<Operation>();
        int migrationOperationSize = in.readInt();
        for (int i = 0; i < migrationOperationSize; i++) {
            Operation migrationOperation = in.readObject();
            migrationOperations.add(migrationOperation);
        }
    }

}
