package com.hazelcast.partition.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.InternalPartitionLostEvent;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.PartitionAwareService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;

import static com.hazelcast.partition.impl.PartitionReplicaChangeReason.ASSIGNMENT;
import static com.hazelcast.partition.impl.PartitionReplicaChangeReason.MEMBER_REMOVED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ForceUpdateSyncWaitingReplicaVersionsOperationTest {

    @Mock
    private InternalPartitionService partitionService;

    @Mock
    private NodeEngineImpl nodeEngine;

    @Mock
    private PartitionAwareService partitionAwareService;

    @Before
    public void before() {
        when(nodeEngine.getService("partitionService")).thenReturn(partitionService);
        when(nodeEngine.getLogger(ForceUpdateSyncWaitingReplicaVersionsOperation.class)).thenReturn(mock(ILogger.class));
        when(nodeEngine.getServices(PartitionAwareService.class)).thenReturn(Arrays.asList(partitionAwareService));
    }

    @Test
    public void test_partitionLostEventDispatched_whenFirstNodeFailed()
            throws Exception {
        final long[] versions = {6, 5, 4, 3, 2, 1};
        final long[] expectedVersions = {6, 5, 4, 3, 2, 1};
        final int partitionId = 1;

        final ArgumentCaptor<InternalPartitionLostEvent> eventCaptor = ArgumentCaptor.forClass(InternalPartitionLostEvent.class);

        invokeOperation(versions, partitionId, MEMBER_REMOVED);
        assertThat(versions, equalTo(expectedVersions));

        verifyInternalPartitionLostEvent(partitionId, 0, eventCaptor);
    }

    @Test
    public void test_partitionLostEventDispatched_whenSecondNodeFailed()
            throws Exception {
        final long[] versions = {-1, 5, 4, 3, 2, 1};
        final long[] expectedVersions = {5, 5, 4, 3, 2, 1};
        final int partitionId = 1;

        final ArgumentCaptor<InternalPartitionLostEvent> eventCaptor = ArgumentCaptor.forClass(InternalPartitionLostEvent.class);

        invokeOperation(versions, partitionId, MEMBER_REMOVED);
        assertThat(versions, equalTo(expectedVersions));

        verifyInternalPartitionLostEvent(partitionId, 1, eventCaptor);
    }

    @Test
    public void test_partitionLostEventDispatched_whenAllNodesFailed()
            throws Exception {
        final long[] versions = {-1, -1, -1, -1, -1, -1};
        final long[] expectedVersions = {0, 0, 0, 0, 0, 0};
        final int partitionId = 1;

        final ArgumentCaptor<InternalPartitionLostEvent> eventCaptor = ArgumentCaptor.forClass(InternalPartitionLostEvent.class);

        invokeOperation(versions, partitionId, MEMBER_REMOVED);
        assertThat(versions, equalTo(expectedVersions));

        verifyInternalPartitionLostEvent(partitionId, 6, eventCaptor);
    }

    @Test
    public void test_partitionLostEventNotDispatched_whenPartitionAssignmentIsDone()
            throws Exception {
        final long[] versions = {0, -1, -1, 3, 2, 1};
        final long[] expectedVersions = {0, 0, 0, 3, 2, 1};
        final int partitionId = 1;

        invokeOperation(versions, partitionId, ASSIGNMENT);
        assertThat(versions, equalTo(expectedVersions));

        verify(partitionAwareService, never()).onPartitionLost(any(InternalPartitionLostEvent.class));
    }

    private void verifyInternalPartitionLostEvent(final int partitionId, final int replicaIndex,
                                                  final ArgumentCaptor<InternalPartitionLostEvent> eventCaptor) {
        verify(partitionAwareService).onPartitionLost(eventCaptor.capture());
        final InternalPartitionLostEvent expectedEvent = eventCaptor.getValue();
        assertNotNull(expectedEvent);
        assertEquals(partitionId, expectedEvent.getPartitionId());
        assertEquals(replicaIndex, expectedEvent.getLostReplicaIndex());
    }

    private void invokeOperation(long[] versions, int partitionId, PartitionReplicaChangeReason reason)
            throws Exception {
        final ForceUpdateSyncWaitingReplicaVersionsOperation operation = createOperation(partitionId, reason);
        when(partitionService.getPartitionReplicaVersions(partitionId)).thenReturn(versions);
        operation.run();
    }

    private ForceUpdateSyncWaitingReplicaVersionsOperation createOperation(final int partitionId,
                                                                  final PartitionReplicaChangeReason reason) {
        final ForceUpdateSyncWaitingReplicaVersionsOperation operation = new ForceUpdateSyncWaitingReplicaVersionsOperation(reason);
        operation.setPartitionId(partitionId);
        operation.setNodeEngine(nodeEngine);
        operation.setServiceName("partitionService");
        return operation;
    }

}
