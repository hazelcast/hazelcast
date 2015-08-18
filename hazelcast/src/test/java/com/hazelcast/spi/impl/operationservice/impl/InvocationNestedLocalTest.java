package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.OperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationNestedLocalTest extends InvocationNestedTest {

    private final String response = "someresponse";

    @Test
    public void whenPartition_callsGeneric() {
        HazelcastInstance hz = createHazelcastInstance();
        OperationService operationService = getOperationService(hz);

        InnerOperation innerOperation = new InnerOperation(response, -1);
        OuterOperation outerOperation = new OuterOperation(innerOperation, 0);

        InternalCompletableFuture f = operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());

        assertEquals(response, f.getSafely());
    }

    @Test
    public void whenPartition_callsCorrectPartition() {
        HazelcastInstance hz = createHazelcastInstance();
        OperationService operationService = getOperationService(hz);

        int partitionId = 0;
        InnerOperation innerOperation = new InnerOperation(response, partitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, partitionId);

        InternalCompletableFuture f = operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());

        assertEquals(response, f.getSafely());
    }

    @Test
    public void whenPartition_callsIncorrectPartition() {
        HazelcastInstance hz = createHazelcastInstance();
        OperationService operationService = getOperationService(hz);

        int outerPartitionId = 0;
        int innerPartitionId = 1;
        for (; innerPartitionId < hz.getPartitionService().getPartitions().size(); innerPartitionId++) {
            if (!mappedToSameThread(operationService, outerPartitionId, innerPartitionId)) {
                break;
            }
        }

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
    public void whenPartition_callsDifferentPartition_butMappedToSameThread() throws ExecutionException, InterruptedException {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT, "2");
        config.setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT, "1");
        HazelcastInstance hz = createHazelcastInstance(config);
        final OperationService operationService = getOperationService(hz);

        int innerPartitionId = 0;
        int outerPartitionId = 1;
        InnerOperation innerOperation = new InnerOperation(response, innerPartitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, outerPartitionId);

        Future f = operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());
        assertEquals(response, f.get());
    }

    @Test
    public void whenGeneric_callsGeneric() {
        HazelcastInstance hz = createHazelcastInstance();
        OperationService operationService = getOperationService(hz);

        InnerOperation innerOperation = new InnerOperation(response, -1);
        OuterOperation outerOperation = new OuterOperation(innerOperation, -1);

        InternalCompletableFuture f = operationService.invokeOnTarget(null, outerOperation, getAddress(hz));

        assertEquals(response, f.getSafely());
    }

    @Test
    public void whenGeneric_callsPartitionSpecific() {
        HazelcastInstance hz = createHazelcastInstance();
        OperationService operationService = getOperationService(hz);

        int innerPartitionId = 0;
        InnerOperation innerOperation = new InnerOperation(response, innerPartitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, -1);

        InternalCompletableFuture f = operationService.invokeOnTarget(null, outerOperation, getAddress(hz));

        assertEquals(response, f.getSafely());
    }
}
