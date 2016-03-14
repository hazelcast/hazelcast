package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class Invocation_NestedRemoteTest extends Invocation_NestedAbstractTest {

    private static final String RESPONSE = "someresponse";

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void invokeOnPartition_outerGeneric_innerGeneric_forbidden() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(1).newInstances();
        HazelcastInstance local = cluster[0];
        OperationService operationService = getOperationService(local);

        InnerOperation innerOperation = new InnerOperation(RESPONSE, GENERIC_OPERATION);
        OuterOperation outerOperation = new OuterOperation(innerOperation, GENERIC_OPERATION);

        expected.expect(Exception.class);
        operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());
    }

    @Test
    public void invokeOnPartition_outerRemote_innerGeneric() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        InnerOperation innerOperation = new InnerOperation(RESPONSE, GENERIC_OPERATION);
        OuterOperation outerOperation = new OuterOperation(innerOperation, getPartitionId(remote));
        InternalCompletableFuture f = operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());

        assertEquals(RESPONSE, f.join());
    }

    @Test
    public void invokeOnPartition_outerRemote_innerSameInstance_samePartition() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        int partitionId = getPartitionId(remote);
        InnerOperation innerOperation = new InnerOperation(RESPONSE, partitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, partitionId);
        InternalCompletableFuture f = operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());

        assertEquals(RESPONSE, f.join());
    }

    @Test
    public void invokeOnPartition_outerRemote_innerSameInstance_callsDifferentPartition_mappedToSameThread() throws Exception {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        int outerPartitionId = getPartitionId(remote);
        int innerPartitionId = randomPartitionIdMappedToSameThreadAsGivenPartitionIdOnInstance(
                outerPartitionId, remote, operationService);
        InnerOperation innerOperation = new InnerOperation(RESPONSE, innerPartitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, outerPartitionId);
        InternalCompletableFuture f = operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());

        expected.expect(IllegalThreadStateException.class);
        expected.expectMessage("cannot make remote call");
        f.join();
    }

    @Test
    public void invokeOnPartition_outerRemote_innerDifferentInstance_forbidden() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        int outerPartitionId = getPartitionId(remote);
        int innerPartitionId = getPartitionId(local);
        assertNotEquals("partitions should be different", innerPartitionId, outerPartitionId);
        InnerOperation innerOperation = new InnerOperation(RESPONSE, innerPartitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, outerPartitionId);
        InternalCompletableFuture f = operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());

        expected.expect(IllegalThreadStateException.class);
        expected.expectMessage("cannot make remote call");
        f.join();
    }

    @Test
    public void invokeOnPartition_outerLocal_innerDifferentInstance_forbidden() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        int outerPartitionId = getPartitionId(local);
        int innerPartitionId = getPartitionId(remote);
        assertNotEquals("partitions should be different", innerPartitionId, outerPartitionId);
        InnerOperation innerOperation = new InnerOperation(RESPONSE, innerPartitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, outerPartitionId);
        InternalCompletableFuture f = operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());

        expected.expect(IllegalThreadStateException.class);
        expected.expectMessage("cannot make remote call");
        f.join();
    }

    @Test
    public void invokeOnTarget_outerGeneric_innerGeneric() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        InnerOperation innerOperation = new InnerOperation(RESPONSE, GENERIC_OPERATION);
        OuterOperation outerOperation = new OuterOperation(innerOperation, GENERIC_OPERATION);
        InternalCompletableFuture f = operationService.invokeOnTarget(null, outerOperation, getAddress(remote));

        assertEquals(RESPONSE, f.join());
    }

    @Test
    public void invokeOnTarget_outerGeneric_innerSameInstance() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        InnerOperation innerOperation = new InnerOperation(RESPONSE, 0);
        OuterOperation outerOperation = new OuterOperation(innerOperation, GENERIC_OPERATION);
        InternalCompletableFuture f = operationService.invokeOnTarget(null, outerOperation, getAddress(remote));

        assertEquals(RESPONSE, f.join());
    }

}
