package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.operationexecutor.classic.ClassicOperationExecutor;
import com.hazelcast.test.HazelcastTestSupport;

import java.io.IOException;

public abstract class InvocationNestedTest extends HazelcastTestSupport {

    public static boolean mappedToSameThread(OperationService operationService, int partitionId1, int partitionId2){
        OperationServiceImpl operationServiceImpl = (OperationServiceImpl) operationService;
        ClassicOperationExecutor executor = (ClassicOperationExecutor) operationServiceImpl.getOperationExecutor();
        int thread1 =  executor.toPartitionThreadIndex(partitionId1);
        int thread2 =  executor.toPartitionThreadIndex(partitionId2);
        return thread1 == thread2;
    }

    public static class OuterOperation extends AbstractOperation {
        public Operation innerOperation;
        public Object result;

        public OuterOperation() {
        }

        public OuterOperation(Operation innerOperation, int partitionId) {
            this.innerOperation = innerOperation;
            setPartitionId(partitionId);
        }

        @Override
        public void run() throws Exception {
            System.out.println(Thread.currentThread());
            int partitionId = innerOperation.getPartitionId();
            OperationService operationService = getNodeEngine().getOperationService();
            InternalCompletableFuture f;
            if (partitionId >= 0) {
                f = operationService.invokeOnPartition(null, innerOperation, partitionId);
            } else {
                f = operationService.invokeOnTarget(null, innerOperation, getNodeEngine().getThisAddress());
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
            out.writeObject(innerOperation);

        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            innerOperation = in.readObject();
        }
    }

    public static class InnerOperation extends AbstractOperation {
        public Object value;

        public InnerOperation() {
        }

        public InnerOperation(Object value, int partitionId) {
            this.value = value;
            this.setPartitionId(partitionId);
        }

        @Override
        public void run() throws Exception {
            System.out.println(Thread.currentThread());
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
