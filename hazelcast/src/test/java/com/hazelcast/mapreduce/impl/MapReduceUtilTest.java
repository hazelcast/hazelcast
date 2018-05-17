package com.hazelcast.mapreduce.impl;

import com.hazelcast.mapreduce.impl.operation.NotifyRemoteExceptionOperation;
import com.hazelcast.mapreduce.impl.task.JobProcessInformationImpl;
import com.hazelcast.mapreduce.impl.task.JobSupervisor;
import com.hazelcast.mapreduce.impl.task.JobTaskConfiguration;
import com.hazelcast.mapreduce.impl.task.MemberAssigningJobProcessInformationImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeoutException;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapReduceUtilTest {

    @Test
    public void testNotifyRemoteException() {
        JobSupervisor jobSupervisor = mock(JobSupervisor.class, RETURNS_DEEP_STUBS);
        MapReduceService mapReduceService = mock(MapReduceService.class);
        NodeEngine nodeEngine = mock(NodeEngine.class);
        OperationService operationService = mock(OperationService.class);

        Throwable throwable = new Throwable();

        when(jobSupervisor.getMapReduceService()).thenReturn(mapReduceService);
        when(jobSupervisor.isOwnerNode()).thenReturn(false);
        when(mapReduceService.getNodeEngine()).thenReturn(nodeEngine);
        when(nodeEngine.getOperationService()).thenReturn(operationService);

        when(jobSupervisor.getConfiguration().getName()).thenReturn("name");
        when(jobSupervisor.getConfiguration().getJobId()).thenReturn("id");

        MapReduceUtil.notifyRemoteException(jobSupervisor, throwable);

        verify(operationService).send(any(NotifyRemoteExceptionOperation.class), any(Address.class));
        verify(jobSupervisor).isOwnerNode();
        verify(nodeEngine, never()).getLogger(any(Class.class));
        verify(jobSupervisor, never())
                .notifyRemoteException(any(Address.class), any(Throwable.class));
    }

    @Test
    public void testEnforcePartitionTableWarmUp() throws TimeoutException {
        // we make sure that we do not exceed PARTITION_READY_TIMEOUT
        final int partitionCount = 2;
        final int expectedCheckCount = 4;

        MapReduceService mapReduceService = mock(MapReduceService.class, RETURNS_DEEP_STUBS);
        IPartitionService partitionService = mock(IPartitionService.class);

        when(mapReduceService.getNodeEngine().getPartitionService()).thenReturn(partitionService);
        when(partitionService.getPartitionCount()).thenReturn(partitionCount);
        when(partitionService.getPartitionOwner(anyInt()))
                .thenReturn(null)
                .thenReturn(null)
                .thenReturn(new Address());

        MapReduceUtil.enforcePartitionTableWarmup(mapReduceService);

        verify(partitionService, times(expectedCheckCount)).getPartitionOwner(anyInt());
    }

    @Test
    public void testCreateJobProcessInformation_whenKeyValueSourceIsNotPartitionIdAware() {
        JobTaskConfiguration configuration = mock(JobTaskConfiguration.class);
        JobSupervisor supervisor = mock(JobSupervisor.class);
        NodeEngine nodeEngine = mock(NodeEngine.class, RETURNS_DEEP_STUBS);

        when(configuration.getNodeEngine()).thenReturn(nodeEngine);
        when(configuration.getKeyValueSource()).thenReturn(new ListKeyValueSource());
        when(nodeEngine.getClusterService().getSize(DATA_MEMBER_SELECTOR)).thenReturn(42);

        JobProcessInformationImpl actual = MapReduceUtil.createJobProcessInformation(configuration, supervisor);
        assertTrue(actual instanceof MemberAssigningJobProcessInformationImpl);

        verify(nodeEngine, never()).getPartitionService();
    }
}