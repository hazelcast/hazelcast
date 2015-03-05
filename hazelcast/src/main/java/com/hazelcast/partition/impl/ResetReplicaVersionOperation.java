/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.partition.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationCycleOperation;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.Arrays;

// runs locally...
final class ResetReplicaVersionOperation extends AbstractOperation
        implements PartitionAwareOperation, MigrationCycleOperation {

    @Override
    public void run() throws Exception {
        int partitionId = getPartitionId();
        InternalPartitionService partitionService = getService();
        long[] versions = partitionService.getPartitionReplicaVersions(partitionId);
        // InternalPartitionService.getPartitionReplicaVersions() returns internal
        // version array, we need to clone it here
        versions = Arrays.copyOf(versions, InternalPartition.MAX_BACKUP_COUNT);

        // clear and set replica versions back to ensure backup replica does not have
        // version numbers of prior replicas
        partitionService.clearPartitionReplicaVersions(partitionId);
        partitionService.setPartitionReplicaVersions(partitionId, versions, getReplicaIndex());
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
}
