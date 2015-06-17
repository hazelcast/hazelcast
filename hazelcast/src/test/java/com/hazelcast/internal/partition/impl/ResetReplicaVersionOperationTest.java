package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.partition.impl.PartitionReplicaChangeReason;
import com.hazelcast.internal.partition.impl.ResetReplicaVersionOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.hazelcast.internal.partition.impl.PartitionReplicaChangeReason.ASSIGNMENT;
import static com.hazelcast.internal.partition.impl.PartitionReplicaChangeReason.MEMBER_REMOVED;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ResetReplicaVersionOperationTest {

    @Mock
    private InternalPartitionService partitionService;

    @Mock
    private NodeEngineImpl nodeEngine;

    @Before
    public void before() {
        when(nodeEngine.getService("partitionService")).thenReturn(partitionService);
        when(nodeEngine.getLogger(ResetReplicaVersionOperation.class)).thenReturn(mock(ILogger.class));
    }

    @Test
    public void test_setSyncWaitingFlag_whenMemberRemoved()
            throws Exception {
        final long[] versions = {6, 5, 4, 3, 2, 1};
        final long[] updatedVersions = {-1, 5, 4, 3, 2, 1};

        testResetReplicaVersionOperation(1, 1, versions, updatedVersions, MEMBER_REMOVED, false);
    }

    @Test
    public void test_setSyncWaitingFlag_whenAssignmentsAreDone()
            throws Exception {
        final long[] versions = {6, 5, 4, 3, 2, 1};
        final long[] updatedVersions = {-1, 5, 4, 3, 2, 1};

        testResetReplicaVersionOperation(1, 1, versions, updatedVersions, ASSIGNMENT, false);
    }

    @Test
    public void test_notSetSyncWaitingFlag_whenAssignmentsAreDone()
            throws Exception {
        final long[] versions = {6, 5, 4, 3, 2, 1};

        testResetReplicaVersionOperation(1, 1, versions, versions, ASSIGNMENT, true);
    }

    @Test
    public void test_keepPreviousSyncWaitingFlags_whenMemberRemoved()
            throws Exception {
        final long[] versions = {6, -1, -1, 3, 2, 1};
        final long[] updatedVersions = {-1, -1, -1, 3, 2, 1};

        testResetReplicaVersionOperation(1, 1, versions, updatedVersions, MEMBER_REMOVED, false);
    }

    @Test
    public void test_resetPreviousSyncWaitingFlags_whenAssignmentsAreDone()
            throws Exception {
        final long[] versions = {6, -1, -1, 3, 2, 1};
        final long[] updatedVersions = {-1, 0, 0, 3, 2, 1};

        testResetReplicaVersionOperation(1, 1, versions, updatedVersions, ASSIGNMENT, false);
    }

    private void testResetReplicaVersionOperation(final int partitionId, final int replicaIndex, final long[] versions,
                                                  final long[] updatedVersions, final PartitionReplicaChangeReason reason,
                                                  final boolean initialAssignment)
            throws Exception {
        final ResetReplicaVersionOperation operation = createOperation(partitionId, replicaIndex, reason, initialAssignment);

        when(partitionService.getPartitionReplicaVersions(partitionId)).thenReturn(versions);

        operation.run();

        verify(partitionService).clearPartitionReplicaVersions(partitionId);
        verify(partitionService).setPartitionReplicaVersions(partitionId, updatedVersions, replicaIndex);
    }

    private ResetReplicaVersionOperation createOperation(final int partitionId, final int replicaIndex,
                                                         final PartitionReplicaChangeReason reason,
                                                         final boolean initialAssignment) {
        final ResetReplicaVersionOperation operation = new ResetReplicaVersionOperation(reason, initialAssignment);
        operation.setReplicaIndex(replicaIndex);
        operation.setPartitionId(partitionId);
        operation.setNodeEngine(nodeEngine);
        operation.setServiceName("partitionService");
        return operation;
    }

}
