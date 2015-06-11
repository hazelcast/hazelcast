package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.OperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test how well HZ deals with nested invocations.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class InvocationNestedRemoteTest extends InvocationNestedTest {

    private final String response = "someresponse";

    // ======================= remote invocations ============================================

    @Test
    public void whenPartition_callsGeneric() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        InnerOperation innerOperation = new InnerOperation(response, -1);
        OuterOperation outerOperation = new OuterOperation(innerOperation, getPartitionId(remote));

        InternalCompletableFuture f = operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());

        assertEquals(response, f.getSafely());
    }

    @Test
    public void whenPartition_callsSamePartition() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        int partitionId = getPartitionId(remote);
        InnerOperation innerOperation = new InnerOperation(response, partitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, partitionId);

        InternalCompletableFuture f = operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());

        assertEquals(response, f.getSafely());
    }

    @Test
    public void whenPartition_callsDifferentPartition_butMappedToSameThread() throws ExecutionException, InterruptedException {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        int outerPartitionId = getPartitionId(remote);
        int innerPartitionId = 0;
        for (; innerPartitionId < remote.getPartitionService().getPartitions().size(); innerPartitionId++) {
            if (innerPartitionId == outerPartitionId) {
                continue;
            }

            if (!getPartitionService(remote).getPartition(innerPartitionId).isLocal()) {
                continue;
            }

            if (mappedToSameThread(operationService, outerPartitionId, innerPartitionId)) {
                break;
            }
        }
        InnerOperation innerOperation = new InnerOperation(response, innerPartitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, outerPartitionId);

        InternalCompletableFuture f = operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());

        assertEquals(response, f.getSafely());
    }

    @Test
    public void whenPartition_callsIncorrectPartition() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        int outerPartitionId = getPartitionId(remote);
        int innerPartitionId = getPartitionId(local);
        InnerOperation innerOperation = new InnerOperation(response, innerPartitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, outerPartitionId);

        InternalCompletableFuture f = operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());

        try {
            f.getSafely();
            fail();
        } catch (IllegalThreadStateException e) {
        }
    }

    @Test
    public void whenGeneric_callsGeneric() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        InnerOperation innerOperation = new InnerOperation(response, -1);
        OuterOperation outerOperation = new OuterOperation(innerOperation, -1);

        InternalCompletableFuture f = operationService.invokeOnTarget(null, outerOperation, getAddress(remote));

        assertEquals(response, f.getSafely());
    }

    @Test
    public void whenGeneric_callsPartition() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        InnerOperation innerOperation = new InnerOperation(response, 0);
        OuterOperation outerOperation = new OuterOperation(innerOperation, -1);

        InternalCompletableFuture f = operationService.invokeOnTarget(null, outerOperation, getAddress(remote));

        assertEquals(response, f.getSafely());
    }
}
