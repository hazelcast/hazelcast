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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import java.io.IOException;

import static com.hazelcast.internal.partition.MigrationEndpoint.DESTINATION;

// Runs locally when the node becomes owner of a partition
abstract class AbstractPromotionOperation extends AbstractPartitionOperation
        implements PartitionAwareOperation, MigrationCycleOperation {

    // this is the replica index of the partition owner before the promotion.
    protected final MigrationInfo migrationInfo;

    AbstractPromotionOperation(MigrationInfo migrationInfo) {
        this.migrationInfo = migrationInfo;
    }

    PartitionMigrationEvent getPartitionMigrationEvent() {
        return new PartitionMigrationEvent(DESTINATION, getPartitionId(), migrationInfo.getDestinationCurrentReplicaIndex(),
                0, migrationInfo.getUid());
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getClassId() {
        throw new UnsupportedOperationException();
    }
}
