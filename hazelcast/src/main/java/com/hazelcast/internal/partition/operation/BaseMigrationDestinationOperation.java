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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalMigrationListener.MigrationParticipant;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.partition.MigrationEndpoint;

import java.util.logging.Level;

import static com.hazelcast.internal.cluster.Versions.V3_9;

abstract class BaseMigrationDestinationOperation extends BaseMigrationOperation {

    private static final OperationResponseHandler ERROR_RESPONSE_HANDLER = new OperationResponseHandler() {
        @Override
        public void sendResponse(Operation op, Object obj) {
            throw new HazelcastException("Migration operations can not send response!");
        }
    };

    Throwable failureReason;

    BaseMigrationDestinationOperation() {
    }

    BaseMigrationDestinationOperation(MigrationInfo migrationInfo, int partitionStateVersion) {
        super(migrationInfo, partitionStateVersion);
    }

    @Override
    protected PartitionMigrationEvent getMigrationEvent() {
        return new PartitionMigrationEvent(MigrationEndpoint.DESTINATION,
                migrationInfo.getPartitionId(), migrationInfo.getDestinationCurrentReplicaIndex(),
                migrationInfo.getDestinationNewReplicaIndex());
    }

    @Override
    protected MigrationParticipant getMigrationParticipantType() {
        return MigrationParticipant.DESTINATION;
    }

    protected void prepareOperation(Operation op) {
        op.setNodeEngine(getNodeEngine())
          .setPartitionId(getPartitionId())
          .setReplicaIndex(getReplicaIndex());
        op.setOperationResponseHandler(ERROR_RESPONSE_HANDLER);
        OperationAccessor.setCallerAddress(op, migrationInfo.getSource());
    }

    final void verifyMasterOnMigrationDestination() {
        NodeEngine nodeEngine = getNodeEngine();
        Address masterAddress = nodeEngine.getMasterAddress();
        if (!masterAddress.equals(migrationInfo.getMaster())) {
            ClusterService clusterService = nodeEngine.getClusterService();
            if (clusterService.getClusterVersion().isGreaterOrEqual(V3_9)) {
                throw new IllegalStateException("Migration initiator is not master node! => " + toString());
            } else {
                throw new RetryableHazelcastException("Migration initiator is not master node! => " + toString());
            }
        }
    }

    final void runMigrationOperation(Operation op) throws Exception {
        prepareOperation(op);
        op.beforeRun();
        op.run();
        op.afterRun();
    }

    final void logMigrationCancelled() {
        getLogger().warning("Migration is cancelled -> " + migrationInfo);
    }

    final void logMigrationFailure(Throwable e) {
        Level level = Level.WARNING;
        if (e instanceof IllegalStateException) {
            level = Level.FINEST;
        }
        ILogger logger = getLogger();
        if (logger.isLoggable(level)) {
            logger.log(level, e.getMessage(), e);
        }
    }
}
