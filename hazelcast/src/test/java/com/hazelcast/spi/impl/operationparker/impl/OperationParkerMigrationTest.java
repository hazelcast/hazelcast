/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationparker.impl;

import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.ReplicaSyncEvent;
import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.BlockingBackupOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationResponseHandler;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static com.hazelcast.test.Accessors.getNode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * These tests check only reasonable/expected/valid combinations of parked operation types (primary or backup)
 * and migration events.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationParkerMigrationTest extends HazelcastTestSupport {

    private static final int MIGRATING_PARTITION_ID = 1;
    private static final int BACKUP_COUNT = 3;
    // "middle" backup
    private static final int BACKUP_REPLICA_INDEX = BACKUP_COUNT - 1;
    private static final int NOT_NEEDED_BACKUP_REPLICA_INDEX = BACKUP_COUNT + 1;

    NodeEngineImpl nodeEngine;
    OperationParkerImpl operationParker;
    OperationResponseHandler responseHandler = mock(OperationResponseHandler.class);

    @Before
    public void setup() {
        var hz = createHazelcastInstance();
        nodeEngine = getNode(hz).nodeEngine;
        operationParker = (OperationParkerImpl) nodeEngine.getOperationParker();
    }

    @Test
    public void shouldInvalidatePrimaryOperation_onOwnerToBackupSource() {
        var op = givenParkedOperation();

        // when
        // this member will no longer be the owner
        operationParker.onPartitionMigrate(new PartitionMigrationEvent(
                MigrationEndpoint.SOURCE, MIGRATING_PARTITION_ID, 0, BACKUP_REPLICA_INDEX, UUID.randomUUID()));

        // then
        unpark(op);
        assertThatOperationWasInvalidated(op);
    }

    @Test
    public void shouldInvalidatePrimaryOperation_onPartitionRemovedOwnerSource() {
        var op = givenParkedOperation();

        // when
        // this member will no longer be the owner nor backup
        operationParker.onPartitionMigrate(new PartitionMigrationEvent(
                MigrationEndpoint.SOURCE, MIGRATING_PARTITION_ID, 0, -1, UUID.randomUUID()));

        // then
        unpark(op);
        assertThatOperationWasInvalidated(op);
    }

    @Test
    public void shouldDiscardBackupOperation_onPromotionDestination() {
        var op = givenParkedBackupOperation();

        // when
        //
        // This handles the following cases:
        // - regular migration that changes the owner - on new owner
        // - backup promotion because of owner loss - on new owner
        //
        // Note: it might be tempting to keep the backup operations in case owner crashed to limit data loss.
        // However, this would cause significant complications. And the ACKs have not yet been sent so the data
        // is not guaranteed to be safe.
        operationParker.onPartitionMigrate(new PartitionMigrationEvent(
                MigrationEndpoint.DESTINATION, MIGRATING_PARTITION_ID, BACKUP_REPLICA_INDEX, 0, UUID.randomUUID()));

        // then
        unpark(op);
        assertThatOperationWasDiscarded(op);
    }

    @Test
    public void shouldDiscardBackupOperation_onMigrationDestination() {
        var op = givenParkedBackupOperation();

        // when
        operationParker.onPartitionMigrate(new PartitionMigrationEvent(
                MigrationEndpoint.DESTINATION, MIGRATING_PARTITION_ID, BACKUP_REPLICA_INDEX, 0, UUID.randomUUID()));

        // then
        unpark(op);
        assertThatOperationWasDiscarded(op);
    }

    @Test
    public void shouldExecuteBackupOperation_onBackupToBackupSource() {
        var op = givenParkedBackupOperation();

        // when
        operationParker.onPartitionMigrate(new PartitionMigrationEvent(
                MigrationEndpoint.SOURCE, MIGRATING_PARTITION_ID, BACKUP_REPLICA_INDEX, BACKUP_REPLICA_INDEX - 1, UUID.randomUUID()));

        // then
        assertThat(op.getReplicaIndex()).as("Should update replica index").isEqualTo(BACKUP_REPLICA_INDEX - 1);
        unpark(op);
        assertThatOperationWasExecuted(op);
    }

    @Test
    public void shouldDiscardBackupOperation_onBackupToNotNeededBackupSource() {
        var op = givenParkedBackupOperation();

        // when
        operationParker.onPartitionMigrate(new PartitionMigrationEvent(
                MigrationEndpoint.SOURCE, MIGRATING_PARTITION_ID, BACKUP_REPLICA_INDEX, NOT_NEEDED_BACKUP_REPLICA_INDEX, UUID.randomUUID()));

        // then
        unpark(op);
        assertThatOperationWasDiscarded(op);
    }

    @Test
    public void shouldDiscardBackupOperation_onReplicaSyncDestination_whenNamespaceMatches() {
        var op = givenParkedBackupOperation();


        // when
        operationParker.onReplicaSync(new ReplicaSyncEvent(MIGRATING_PARTITION_ID,
                new DistributedObjectNamespace("service", "dummy_name"), BACKUP_REPLICA_INDEX));

        // then
        unpark(op);
        assertThatOperationWasDiscarded(op);
    }

    @Test
    public void shouldExecuteBackupOperation_onReplicaSyncDestination_whenNamespaceDoesNotMatch() {
        var op = givenParkedBackupOperation();

        // when
        operationParker.onReplicaSync(new ReplicaSyncEvent(MIGRATING_PARTITION_ID,
                new DistributedObjectNamespace("service", "different_name"), BACKUP_REPLICA_INDEX));

        // then
        unpark(op);
        assertThatOperationWasExecuted(op);
    }

    //////////////////////////// test DSL

    private WaitSetTest.BlockedOperation givenParkedOperation() {
        var op = new WaitSetTest.BlockedOperation();
        op.setServiceName("service");
        op.objectId = "dummy_name";
        op.setPartitionId(MIGRATING_PARTITION_ID).setReplicaIndex(0);
        op.setOperationResponseHandler(responseHandler);

        operationParker.park(op);
        return op;
    }

    private BlockedBackupOperation givenParkedBackupOperation() {
        var op = new BlockedBackupOperation();
        op.setServiceName("service");
        op.objectId = "dummy_name";
        op.setPartitionId(MIGRATING_PARTITION_ID).setReplicaIndex(BACKUP_REPLICA_INDEX)
                // test executes on 1 member which does not have all replicas.
                // Disable validation in ensureNoPartitionProblems because we do not need in for this test
                .setValidateTarget(false);
        op.setOperationResponseHandler(responseHandler);

        operationParker.park(op);
        return op;
    }

    private void unpark(WaitSetTest.BlockedOperation op) {
        var notifyingOperation = op.createNotifyingOperation();
        var done = new CountDownLatch(1);

        nodeEngine.getOperationService().execute(new PartitionSpecificRunnable() {
            @Override
            public int getPartitionId() {
                return notifyingOperation.getPartitionId();
            }

            @Override
            public void run() {
                // this violates a few rules, but is sufficient for the purpose of the test
                operationParker.unpark(notifyingOperation);
                done.countDown();
            }
        });

        assertOpenEventually(done);
    }

    /**
     * PartitionMigratingException was sent
     */
    private void assertThatOperationWasInvalidated(WaitSetTest.BlockedOperation op) {
        assertThat(op.hasRun).isFalse();
        verify(responseHandler).sendResponse(eq(op), any(PartitionMigratingException.class));
        assertThat(operationParker.getTotalValidWaitingOperationCount()).isZero();
    }

    /**
     * operation was quietly removed
     */
    private void assertThatOperationWasDiscarded(BlockedBackupOperation op) {
        assertThat(op.hasRun).isFalse();
        verifyNoMoreInteractions(responseHandler);
        assertThat(operationParker.getTotalValidWaitingOperationCount()).isZero();
    }


    private void assertThatOperationWasExecuted(WaitSetTest.BlockedOperation op) {
        assertThat(op.hasRun).isTrue();
        verify(responseHandler).sendResponse(op, null);
        assertThat(operationParker.getTotalParkedOperationCount()).isZero();
    }

    static class BlockedBackupOperation extends WaitSetTest.BlockedOperation implements BackupOperation, BlockingBackupOperation {

        @Override
        public boolean shouldKeepAfterMigration(PartitionMigrationEvent event) {
            // simulates structure with given number of backups
            return event.getNewReplicaIndex() <= BACKUP_COUNT;
        }

        @Override
        public void setBackupOpAfterRun(Consumer<Operation> backupOpAfterRun) {
        }
    }
}
