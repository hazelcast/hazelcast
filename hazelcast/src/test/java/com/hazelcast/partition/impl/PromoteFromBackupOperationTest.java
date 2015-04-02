package com.hazelcast.partition.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.InternalPartitionLostEvent;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.impl.PromoteFromBackupOperation.InternalPartitionLostEventPublisher;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.hazelcast.partition.InternalPartitionService.SERVICE_NAME;
import static com.hazelcast.partition.impl.PartitionReplicaChangeReason.ASSIGNMENT;
import static com.hazelcast.partition.impl.PartitionReplicaChangeReason.MEMBER_REMOVED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PromoteFromBackupOperationTest {

    @Mock
    private InternalPartitionService partitionService;

    @Mock
    private NodeEngineImpl nodeEngine;

    @Mock
    private InternalExecutionService executionService;

    @Before
    public void before() {
        when(nodeEngine.getService(SERVICE_NAME)).thenReturn(partitionService);
        when(nodeEngine.getExecutionService()).thenReturn(executionService);
        when(nodeEngine.getLogger(PromoteFromBackupOperation.class)).thenReturn(mock(ILogger.class));
        when(nodeEngine.getLogger(InternalPartitionLostEventPublisher.class)).thenReturn(mock(ILogger.class));
    }

    @Test
    public void test_partitionLostEventDispatched_whenFirstNodeFailed()
            throws Exception {
        final long[] versions = {6, 5, 4, 3, 2, 1};
        final long[] expectedVersions = {6, 5, 4, 3, 2, 1};
        final int partitionId = 1;

        final ArgumentCaptor<InternalPartitionLostEventPublisher> eventCaptor = createArgumentCaptor();

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

        final ArgumentCaptor<InternalPartitionLostEventPublisher> eventCaptor = createArgumentCaptor();

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

        final ArgumentCaptor<InternalPartitionLostEventPublisher> publisherCaptor = createArgumentCaptor();

        invokeOperation(versions, partitionId, MEMBER_REMOVED);
        assertThat(versions, equalTo(expectedVersions));

        verifyInternalPartitionLostEvent(partitionId, 6, publisherCaptor);
    }

    @Test
    public void test_partitionLostEventNotDispatched_whenPartitionAssignmentIsDone()
            throws Exception {
        final long[] versions = {0, -1, -1, 3, 2, 1};
        final long[] expectedVersions = {0, 0, 0, 3, 2, 1};
        final int partitionId = 1;

        invokeOperation(versions, partitionId, ASSIGNMENT);
        assertThat(versions, equalTo(expectedVersions));
    }

    private ArgumentCaptor<InternalPartitionLostEventPublisher> createArgumentCaptor() {
        return ArgumentCaptor.forClass(InternalPartitionLostEventPublisher.class);
    }

    private void verifyInternalPartitionLostEvent(final int partitionId, final int replicaIndex,
                                                  final ArgumentCaptor<InternalPartitionLostEventPublisher> publisherCaptor) {
        verify(executionService).execute(anyString(), publisherCaptor.capture());
        final InternalPartitionLostEventPublisher publisher = publisherCaptor.getValue();
        final InternalPartitionLostEvent expectedEvent = publisher.getEvent();
        assertNotNull(expectedEvent);
        assertEquals(partitionId, expectedEvent.getPartitionId());
        assertEquals(replicaIndex, expectedEvent.getLostReplicaIndex());
    }

    private void invokeOperation(long[] versions, int partitionId, PartitionReplicaChangeReason reason)
            throws Exception {
        final PromoteFromBackupOperation operation = createOperation(partitionId, reason);
        when(partitionService.getPartitionReplicaVersions(partitionId)).thenReturn(versions);
        operation.initLogger();
        operation.handleLostBackups();
    }

    private PromoteFromBackupOperation createOperation(final int partitionId, final PartitionReplicaChangeReason reason) {
        final PromoteFromBackupOperation operation = new PromoteFromBackupOperation(reason, null);
        operation.setPartitionId(partitionId);
        operation.setNodeEngine(nodeEngine);
        operation.setServiceName(SERVICE_NAME);
        return operation;
    }

}
