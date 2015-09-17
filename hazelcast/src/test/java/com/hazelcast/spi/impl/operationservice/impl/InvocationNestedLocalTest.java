package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.OperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationNestedLocalTest extends InvocationNestedTest {

    private static final String RESPONSE = "someresponse";

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void invokeOnPartition_outerGeneric_innerGeneric_forbidden() {
        HazelcastInstance local = createHazelcastInstance();
        OperationService operationService = getOperationService(local);

        InnerOperation innerOperation = new InnerOperation(RESPONSE, GENERIC_OPERATION);
        OuterOperation outerOperation = new OuterOperation(innerOperation, GENERIC_OPERATION);

        expected.expect(Exception.class);
        operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());
    }

    @Test
    public void invokeOnPartition_outerLocal_innerGeneric() {
        HazelcastInstance local = createHazelcastInstance();
        OperationService operationService = getOperationService(local);

        InnerOperation innerOperation = new InnerOperation(RESPONSE, GENERIC_OPERATION);
        OuterOperation outerOperation = new OuterOperation(innerOperation, getPartitionId(local));
        InternalCompletableFuture f = operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());

        assertEquals(RESPONSE, f.getSafely());
    }

    @Test
    public void invokeOnPartition_outerLocal_innerSameInstance_samePartition() {
        HazelcastInstance local = createHazelcastInstance();
        OperationService operationService = getOperationService(local);

        int partitionId = getPartitionId(local);
        InnerOperation innerOperation = new InnerOperation(RESPONSE, partitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, partitionId);
        InternalCompletableFuture f = operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());

        assertEquals(RESPONSE, f.getSafely());
    }

    @Test
    public void invokeOnPartition_outerLocal_innerSameInstance_callsDifferentPartition() {
        HazelcastInstance local = createHazelcastInstance();
        OperationService operationService = getOperationService(local);

        int outerPartitionId = getPartitionId(local);
        int innerPartitionId = randomPartitionIdNotMappedToSameThreadAsGivenPartitionIdOnInstance(local, outerPartitionId);
        InnerOperation innerOperation = new InnerOperation(RESPONSE, innerPartitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, outerPartitionId);
        InternalCompletableFuture f = operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());

        expected.expect(IllegalThreadStateException.class);
        expected.expectMessage("cannot make remote call");
        f.getSafely();
    }

    @Test
    public void invokeOnPartition_outerLocal_innerSameInstance_callsDifferentPartition_mappedToSameThread() throws ExecutionException, InterruptedException {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT, "2");
        config.setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT, "1");
        HazelcastInstance local = createHazelcastInstance(config);
        final OperationService operationService = getOperationService(local);

        int outerPartitionId = 1;
        int innerPartitionId = 0;
        InnerOperation innerOperation = new InnerOperation(RESPONSE, innerPartitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, outerPartitionId);
        InternalCompletableFuture f = operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());

        expected.expect(IllegalThreadStateException.class);
        expected.expectMessage("cannot make remote call");
        f.getSafely();
    }

    @Test
    public void invokeOnTarget_outerGeneric_innerGeneric() {
        HazelcastInstance local = createHazelcastInstance();
        OperationService operationService = getOperationService(local);

        InnerOperation innerOperation = new InnerOperation(RESPONSE, GENERIC_OPERATION);
        OuterOperation outerOperation = new OuterOperation(innerOperation, GENERIC_OPERATION);
        InternalCompletableFuture f = operationService.invokeOnTarget(null, outerOperation, getAddress(local));

        assertEquals(RESPONSE, f.getSafely());
    }

    @Test
    public void invokeOnTarget_outerGeneric_innerSameInstance() {
        HazelcastInstance local = createHazelcastInstance();
        OperationService operationService = getOperationService(local);

        int innerPartitionId = getPartitionId(local);
        InnerOperation innerOperation = new InnerOperation(RESPONSE, innerPartitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, GENERIC_OPERATION);
        InternalCompletableFuture f = operationService.invokeOnTarget(null, outerOperation, getAddress(local));

        assertEquals(RESPONSE, f.getSafely());
    }

}
