/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalMigrationListener.MigrationParticipant;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.partition.MigrationEndpoint;

import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

abstract class BaseMigrationSourceOperation extends BaseMigrationOperation {

    boolean returnResponse = true;

    BaseMigrationSourceOperation() {
    }

    BaseMigrationSourceOperation(MigrationInfo migrationInfo, int partitionStateVersion) {
        super(migrationInfo, partitionStateVersion);
    }

    @Override
    protected PartitionMigrationEvent getMigrationEvent() {
        return new PartitionMigrationEvent(MigrationEndpoint.SOURCE,
                migrationInfo.getPartitionId(), migrationInfo.getSourceCurrentReplicaIndex(),
                migrationInfo.getSourceNewReplicaIndex());
    }

    @Override
    protected MigrationParticipant getMigrationParticipantType() {
        return MigrationParticipant.SOURCE;
    }

    /** Verifies that the local master is equal to the migration master. */
    final void verifyMasterOnMigrationSource() {
        NodeEngine nodeEngine = getNodeEngine();
        Address masterAddress = nodeEngine.getMasterAddress();

        if (!migrationInfo.getMaster().equals(masterAddress)) {
            throw new IllegalStateException("Migration initiator is not master node! => " + toString());
        }

        if (!masterAddress.equals(getCallerAddress())) {
            throw new IllegalStateException("Caller is not master node! => " + toString());
        }
    }

    /** Verifies that this node is the owner of the partition. */
    final void verifySource(Address thisAddress, InternalPartition partition) {
        Address owner = partition.getOwnerOrNull();
        if (owner == null) {
            throw new RetryableHazelcastException("Cannot migrate at the moment! Owner of the partition is null => "
                    + migrationInfo);
        }

        if (!thisAddress.equals(owner)) {
            throw new RetryableHazelcastException("Owner of partition is not this node! => " + toString());
        }
    }

    /** Verifies that the destination is a cluster member. */
    final void verifyExistingTarget(NodeEngine nodeEngine, Address destination) {
        Member target = nodeEngine.getClusterService().getMember(destination);
        if (target == null) {
            throw new TargetNotMemberException("Destination of migration could not be found! => " + toString());
        }
    }

    final PartitionReplicationEvent getPartitionReplicationEvent() {
        return new PartitionReplicationEvent(migrationInfo.getPartitionId(), migrationInfo.getDestinationNewReplicaIndex());
    }

    final void setFailed() {
        success = false;
        onMigrationComplete(false);
    }

    final void completeMigration(boolean result) {
        success = result;
        migrationInfo.doneProcessing();
        onMigrationComplete(result);
        sendResponse(result);
    }

    final void logThrowable(Throwable t) {
        Throwable throwableToLog = t;
        if (throwableToLog instanceof ExecutionException) {
            throwableToLog = throwableToLog.getCause() != null ? throwableToLog.getCause() : throwableToLog;
        }
        Level level = getLogLevel(throwableToLog);
        getLogger().log(level, throwableToLog.getMessage(), throwableToLog);
    }

    private Level getLogLevel(Throwable e) {
        return (e instanceof MemberLeftException || e instanceof InterruptedException)
                || !getNodeEngine().isRunning() ? Level.INFO : Level.WARNING;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    public boolean returnsResponse() {
        return returnResponse;
    }

    @Override
    void executeBeforeMigrations() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        boolean ownerMigration = nodeEngine.getThisAddress().equals(migrationInfo.getSource());
        if (!ownerMigration) {
            return;
        }

        super.executeBeforeMigrations();
    }

}
