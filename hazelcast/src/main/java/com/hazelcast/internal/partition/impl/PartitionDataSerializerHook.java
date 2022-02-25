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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.partition.ReplicaMigrationEventImpl;
import com.hazelcast.internal.partition.MigrationStateImpl;
import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.internal.partition.PartitionLostEventImpl;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.partition.ReplicaFragmentMigrationState;
import com.hazelcast.internal.partition.operation.AssignPartitions;
import com.hazelcast.internal.partition.operation.FetchPartitionStateOperation;
import com.hazelcast.internal.partition.operation.HasOngoingMigration;
import com.hazelcast.internal.partition.operation.MigrationCommitOperation;
import com.hazelcast.internal.partition.operation.MigrationOperation;
import com.hazelcast.internal.partition.operation.MigrationRequestOperation;
import com.hazelcast.internal.partition.operation.PartitionBackupReplicaAntiEntropyOperation;
import com.hazelcast.internal.partition.operation.PartitionReplicaSyncRequest;
import com.hazelcast.internal.partition.operation.PartitionReplicaSyncRequestOffloadable;
import com.hazelcast.internal.partition.operation.PartitionReplicaSyncResponse;
import com.hazelcast.internal.partition.operation.PartitionReplicaSyncRetryResponse;
import com.hazelcast.internal.partition.operation.PartitionStateOperation;
import com.hazelcast.internal.partition.operation.PartitionStateCheckOperation;
import com.hazelcast.internal.partition.operation.PromotionCommitOperation;
import com.hazelcast.internal.partition.operation.PublishCompletedMigrationsOperation;
import com.hazelcast.internal.partition.operation.SafeStateCheckOperation;
import com.hazelcast.internal.partition.operation.ShutdownRequestOperation;
import com.hazelcast.internal.partition.operation.ShutdownResponseOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.util.ConstructorFunction;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.PARTITION_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.PARTITION_DS_FACTORY_ID;

public final class PartitionDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(PARTITION_DS_FACTORY, PARTITION_DS_FACTORY_ID);

    public static final int PARTITION_RUNTIME_STATE = 1;
    public static final int ASSIGN_PARTITIONS = 2;
    public static final int PARTITION_BACKUP_REPLICA_ANTI_ENTROPY = 3;
    public static final int FETCH_PARTITION_STATE = 4;
    public static final int HAS_ONGOING_MIGRATION = 5;
    public static final int MIGRATION_COMMIT = 6;
    public static final int PARTITION_STATE_OP = 7;
    public static final int PROMOTION_COMMIT = 8;
    public static final int REPLICA_SYNC_REQUEST = 9;
    public static final int REPLICA_SYNC_RESPONSE = 10;
    public static final int REPLICA_SYNC_RETRY_RESPONSE = 11;
    public static final int SAFE_STATE_CHECK = 12;
    public static final int SHUTDOWN_REQUEST = 13;
    public static final int SHUTDOWN_RESPONSE = 14;
    public static final int REPLICA_FRAGMENT_MIGRATION_STATE = 15;
    public static final int MIGRATION = 16;
    public static final int MIGRATION_REQUEST = 17;
    public static final int NON_FRAGMENTED_SERVICE_NAMESPACE = 18;
    public static final int PARTITION_REPLICA = 19;
    public static final int PUBLISH_COMPLETED_MIGRATIONS = 20;
    public static final int PARTITION_STATE_CHECK_OP = 21;
    public static final int REPLICA_MIGRATION_EVENT = 22;
    public static final int MIGRATION_EVENT = 23;
    public static final int PARTITION_LOST_EVENT = 24;
    public static final int REPLICA_SYNC_REQUEST_OFFLOADABLE = 25;

    private static final int LEN = REPLICA_SYNC_REQUEST_OFFLOADABLE + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[PARTITION_RUNTIME_STATE] = arg -> new PartitionRuntimeState();
        constructors[ASSIGN_PARTITIONS] = arg -> new AssignPartitions();
        constructors[PARTITION_BACKUP_REPLICA_ANTI_ENTROPY] = arg -> new PartitionBackupReplicaAntiEntropyOperation();
        constructors[FETCH_PARTITION_STATE] = arg -> new FetchPartitionStateOperation();
        constructors[HAS_ONGOING_MIGRATION] = arg -> new HasOngoingMigration();
        constructors[MIGRATION_COMMIT] = arg -> new MigrationCommitOperation();
        constructors[PARTITION_STATE_OP] = arg -> new PartitionStateOperation();
        constructors[PROMOTION_COMMIT] = arg -> new PromotionCommitOperation();
        constructors[REPLICA_SYNC_REQUEST] = arg -> new PartitionReplicaSyncRequest();
        constructors[REPLICA_SYNC_RESPONSE] = arg -> new PartitionReplicaSyncResponse();
        constructors[REPLICA_SYNC_RETRY_RESPONSE] = arg -> new PartitionReplicaSyncRetryResponse();
        constructors[SAFE_STATE_CHECK] = arg -> new SafeStateCheckOperation();
        constructors[SHUTDOWN_REQUEST] = arg -> new ShutdownRequestOperation();
        constructors[SHUTDOWN_RESPONSE] = arg -> new ShutdownResponseOperation();
        constructors[REPLICA_FRAGMENT_MIGRATION_STATE] = arg -> new ReplicaFragmentMigrationState();
        constructors[MIGRATION] = arg -> new MigrationOperation();
        constructors[MIGRATION_REQUEST] = arg -> new MigrationRequestOperation();
        constructors[NON_FRAGMENTED_SERVICE_NAMESPACE] = arg -> NonFragmentedServiceNamespace.INSTANCE;
        constructors[PARTITION_REPLICA] = arg -> new PartitionReplica();
        constructors[PUBLISH_COMPLETED_MIGRATIONS] = arg -> new PublishCompletedMigrationsOperation();
        constructors[PARTITION_STATE_CHECK_OP] = arg -> new PartitionStateCheckOperation();
        constructors[REPLICA_MIGRATION_EVENT] = arg -> new ReplicaMigrationEventImpl();
        constructors[MIGRATION_EVENT] = arg -> new MigrationStateImpl();
        constructors[PARTITION_LOST_EVENT] = arg -> new PartitionLostEventImpl();
        constructors[REPLICA_SYNC_REQUEST_OFFLOADABLE] = arg -> new PartitionReplicaSyncRequestOffloadable();
        return new ArrayDataSerializableFactory(constructors);
    }
}
