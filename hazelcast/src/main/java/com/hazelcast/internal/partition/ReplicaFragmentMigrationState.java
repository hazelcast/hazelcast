package com.hazelcast.internal.partition;

import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
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

// TODO JAVADOC
public class ReplicaFragmentMigrationState implements IdentifiedDataSerializable {

    public static ReplicaFragmentMigrationState newDefaultReplicaFragmentMigrationState(long[] replicaVersions,
                                                                                        Collection<Operation> operations) {
        Map<ReplicaFragmentNamespace, long[]> namespaces = singletonMap(DefaultReplicaFragmentNamespace.INSTANCE,
                replicaVersions);
        return new ReplicaFragmentMigrationState(namespaces, operations);
    }

    public static ReplicaFragmentMigrationState newGroupedReplicaFragmentMigrationState(
            Map<ReplicaFragmentNamespace, long[]> namespaces, Operation operation) {
        return new ReplicaFragmentMigrationState(namespaces, singletonList(operation));
    }

    private Map<ReplicaFragmentNamespace, long[]> namespaces;

    private Collection<Operation> migrationOperations;

    public ReplicaFragmentMigrationState() {
    }

    private ReplicaFragmentMigrationState(Map<ReplicaFragmentNamespace, long[]> namespaces,
                                          Collection<Operation> migrationOperations) {
        this.namespaces = namespaces;
        this.migrationOperations = migrationOperations;
    }

    public Map<ReplicaFragmentNamespace, long[]> getNamespaces() {
        return namespaces;
    }

    public Collection<Operation> getMigrationOperations() {
        return migrationOperations;
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
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
            operation.writeData(out);
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
