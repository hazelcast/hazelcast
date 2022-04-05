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

package com.hazelcast.cp.internal.datastructures.spi;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.operation.unsafe.UnsafeSnapshotReplicationOp;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;

import java.util.Map;

/**
 * Abstract MigrationAwareService implementation to provide partition migration support
 * for CP data structures unsafe mode.
 */
public abstract class AbstractCPMigrationAwareService implements MigrationAwareService {

    protected final NodeEngineImpl nodeEngine;
    private final boolean cpSubsystemEnabled;

    protected AbstractCPMigrationAwareService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.cpSubsystemEnabled = nodeEngine.getConfig().getCPSubsystemConfig().getCPMemberCount() > 0;
    }

    protected abstract int getBackupCount();

    protected abstract Map<CPGroupId, Object> getSnapshotMap(int partitionId);

    protected abstract void clearPartitionReplica(int partitionId);

    @Override
    public final Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (cpSubsystemEnabled) {
            return null;
        }
        if (event.getReplicaIndex() > getBackupCount()) {
            return null;
        }

        Map<CPGroupId, Object> snapshotMap = getSnapshotMap(event.getPartitionId());
        if (snapshotMap.isEmpty()) {
            return null;
        }
        return new UnsafeSnapshotReplicationOp(snapshotMap);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
    }

    @Override
    public final void commitMigration(PartitionMigrationEvent event) {
        if (cpSubsystemEnabled) {
            return;
        }

        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            int thresholdReplicaIndex = event.getNewReplicaIndex();
            if (thresholdReplicaIndex == -1 || thresholdReplicaIndex > getBackupCount()) {
                clearPartitionReplica(event.getPartitionId());
            }
        }
    }

    @Override
    public final void rollbackMigration(PartitionMigrationEvent event) {
        if (cpSubsystemEnabled) {
            return;
        }
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            int thresholdReplicaIndex = event.getCurrentReplicaIndex();
            if (thresholdReplicaIndex == -1 || thresholdReplicaIndex > getBackupCount()) {
                clearPartitionReplica(event.getPartitionId());
            }
        }
    }
}
