package com.hazelcast.spi;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test how well HZ deals with nested invocations.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class InvocationNestedTest extends HazelcastTestSupport {

    // ======================= local invocations ============================================


    @Test
    public void test_whenLocal_andPartitionSpecificCallsGeneric() {
        HazelcastInstance localHz = createHazelcastInstance();

        String response = "someresponse";
        DummyOperation nestedOperation = new DummyOperation(response);
        nestedOperation.setPartitionId(-1);
        NestedOperation operation = new NestedOperation(nestedOperation);
        OperationService operationService = getOperationService(localHz);
        InternalCompletableFuture f = operationService.invokeOnPartition(null, operation, 0);

        assertEquals(response, f.getSafely());
    }

    @Test
    public void test_whenLocal_andPartitionSpecificCallsCorrectPartitionSpecific() {
        HazelcastInstance localHz = createHazelcastInstance();

        String response = "someresponse";
        DummyOperation nestedOperation = new DummyOperation(response);
        nestedOperation.setPartitionId(0);
        NestedOperation operation = new NestedOperation(nestedOperation);
        OperationService operationService = getOperationService(localHz);
        InternalCompletableFuture f = operationService.invokeOnPartition(null, operation, nestedOperation.getPartitionId());

        assertEquals(response, f.getSafely());
    }

    @Test
    public void test_whenLocal_andPartitionSpecificCallsIncorrectPartitionSpecific() {
        HazelcastInstance localHz = createHazelcastInstance();

        String response = "someresponse";
        int outerPartitionId = 0;
        int nestedPartitionId = 1;
        DummyOperation nestedOperation = new DummyOperation(response);
        nestedOperation.setPartitionId(nestedPartitionId);
        NestedOperation operation = new NestedOperation(nestedOperation);
        OperationService operationService = getOperationService(localHz);

        InternalCompletableFuture f = operationService.invokeOnPartition(null, operation, outerPartitionId);

        try {
            f.getSafely();
            fail();
        } catch (IllegalThreadStateException e) {

        }
    }

    @Test
    public void test_whenLocal_andGenericCallsGeneric() {
        HazelcastInstance localHz = createHazelcastInstance();

        String response = "someresponse";
        DummyOperation nestedOperation = new DummyOperation(response);
        nestedOperation.setPartitionId(-1);
        NestedOperation operation = new NestedOperation(nestedOperation);
        OperationService operationService = getOperationService(localHz);
        InternalCompletableFuture f = operationService.invokeOnTarget(null, operation, getAddress(localHz));

        assertEquals(response, f.getSafely());
    }

    @Test
    public void test_whenLocal_andGenericCallsPartitionSpecific() {
        HazelcastInstance localHz = createHazelcastInstance();

        String response = "someresponse";
        DummyOperation nestedOperation = new DummyOperation(response);
        nestedOperation.setPartitionId(0);

        NestedOperation operation = new NestedOperation(nestedOperation);
        OperationService operationService = getOperationService(localHz);
        InternalCompletableFuture f = operationService.invokeOnTarget(null, operation, getAddress(localHz));

        assertEquals(response, f.getSafely());
    }

    // ======================= remote invocations ============================================

    @Test
    public void test_whenRemote_andPartitionSpecificCallsGeneric() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];

        String response = "someresponse";
        DummyOperation nestedOperation = new DummyOperation(response);
        nestedOperation.setPartitionId(-1);
        NestedOperation operation = new NestedOperation(nestedOperation);
        OperationService operationService = getOperationService(local);
        InternalCompletableFuture f = operationService.invokeOnPartition(null, operation, getPartitionId(remote));

        assertEquals(response, f.getSafely());
    }

    @Test
    public void test_whenRemote_andPartitionSpecificCallsCorrectPartitionSpecific() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];

        String response = "someresponse";
        int remotePartitionId = getPartitionId(remote);

        DummyOperation nestedOperation = new DummyOperation(response);
        nestedOperation.setPartitionId(remotePartitionId);

        NestedOperation operation = new NestedOperation(nestedOperation);
        OperationService operationService = getOperationService(local);
        InternalCompletableFuture f = operationService.invokeOnPartition(null, operation, remotePartitionId);

        assertEquals(response, f.getSafely());
    }

    @Test
    public void test_whenRemote_andPartitionSpecificCallsIncorrectPartitionSpecific() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];

        String response = "someresponse";
        int remotePartitionId = getPartitionId(remote);
        int badPartitionId = (remotePartitionId+1)%getNode(local).partitionService.getPartitionCount();

        DummyOperation nestedOperation = new DummyOperation(response);
        nestedOperation.setPartitionId(badPartitionId);

        NestedOperation operation = new NestedOperation(nestedOperation);
        OperationService operationService = getOperationService(local);
        InternalCompletableFuture f = operationService.invokeOnPartition(null, operation, remotePartitionId);


        try {
            f.getSafely();
            fail();
        } catch (IllegalThreadStateException e) {

        }
    }

    @Test
    public void test_whenRemote_andGenericCallsGeneric() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];

        String response = "someresponse";
        DummyOperation nestedOperation = new DummyOperation(response);
        nestedOperation.setPartitionId(-1);
        NestedOperation operation = new NestedOperation(nestedOperation);
        OperationService operationService = getOperationService(local);
        InternalCompletableFuture f = operationService.invokeOnTarget(null, operation, getAddress(remote));

        assertEquals(response, f.getSafely());
    }

    @Test
    public void test_whenRemote_andGenericCallsPartitionSpecific() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];

        String response = "someresponse";
        DummyOperation nestedOperation = new DummyOperation(response);
        nestedOperation.setPartitionId(0);

        NestedOperation operation = new NestedOperation(nestedOperation);
        OperationService operationService = getOperationService(local);
        InternalCompletableFuture f = operationService.invokeOnTarget(null, operation, getAddress(remote));

        assertEquals(response, f.getSafely());
    }

    public static class NestedOperation extends AbstractOperation {
        private Operation nestedOperation;
        private Object result;

        public NestedOperation() {
        }

        public NestedOperation(Operation nestedOperation) {
            this.nestedOperation = nestedOperation;
        }

        @Override
        public void run() throws Exception {
            int partitionId = nestedOperation.getPartitionId();
            OperationService operationService = getNodeEngine().getOperationService();
            InternalCompletableFuture f;
            if (partitionId >= 0) {
                f = operationService.invokeOnPartition(null, nestedOperation, partitionId);
            } else {
                f = operationService.invokeOnTarget(null, nestedOperation, getNodeEngine().getThisAddress());
            }

            result = f.getSafely();
        }

        @Override
        public Object getResponse() {
            return result;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeObject(nestedOperation);

        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            nestedOperation = in.readObject();
        }
    }

    public static class DummyOperation extends AbstractOperation {
        private Object value;

        public DummyOperation() {
        }

        public DummyOperation(Object value) {
            this.value = value;
        }

        @Override
        public void run() throws Exception {

        }

        @Override
        public Object getResponse() {
            return value;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeObject(value);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            value = in.readObject();
        }
    }
}
