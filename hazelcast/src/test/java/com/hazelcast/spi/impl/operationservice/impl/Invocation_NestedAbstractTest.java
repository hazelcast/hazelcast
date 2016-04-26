package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationExecutorImpl;
import com.hazelcast.test.HazelcastTestSupport;

import java.io.IOException;

public abstract class Invocation_NestedAbstractTest extends HazelcastTestSupport {

    protected static final int GENERIC_OPERATION = -1;

    public static boolean mappedToSameThread(OperationService operationService, int partitionId1, int partitionId2) {
        OperationServiceImpl operationServiceImpl = (OperationServiceImpl) operationService;
        OperationExecutorImpl executor = (OperationExecutorImpl) operationServiceImpl.getOperationExecutor();
        int thread1 = executor.toPartitionThreadIndex(partitionId1);
        int thread2 = executor.toPartitionThreadIndex(partitionId2);
        return thread1 == thread2;
    }

    public static class OuterOperation extends AbstractOperation {

        public Operation innerOperation;
        public Object result;

        @SuppressWarnings("unused")
        public OuterOperation() {
        }

        public OuterOperation(Operation innerOperation, int partitionId) {
            this.innerOperation = innerOperation;
            setPartitionId(partitionId);
        }

        @Override
        public void run() throws Exception {
            int partitionId = innerOperation.getPartitionId();
            OperationService operationService = getNodeEngine().getOperationService();
            InternalCompletableFuture f;
            if (partitionId >= 0) {
                f = operationService.invokeOnPartition(null, innerOperation, partitionId);
            } else {
                f = operationService.invokeOnTarget(null, innerOperation, getNodeEngine().getThisAddress());
            }

            result = f.join();
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

        @SuppressWarnings("unused")
        public InnerOperation() {
        }

        public InnerOperation(Object value, int partitionId) {
            this.value = value;

            setPartitionId(partitionId);
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

    protected static int randomPartitionIdNotMappedToSameThreadAsGivenPartitionIdOnInstance(HazelcastInstance hz,
                                                                                            int givenPartitionId) {
        int resultPartitionId;
        for (resultPartitionId = 0; resultPartitionId < hz.getPartitionService().getPartitions().size(); resultPartitionId++) {
            if (resultPartitionId == givenPartitionId) {
                continue;
            }
            if (!getPartitionService(hz).getPartition(resultPartitionId).isLocal()) {
                continue;
            }
            if (!mappedToSameThread(getOperationService(hz), givenPartitionId, resultPartitionId)) {
                break;
            }
        }
        return resultPartitionId;
    }

    protected static int randomPartitionIdMappedToSameThreadAsGivenPartitionIdOnInstance(int givenPartitionId,
                                                                                         HazelcastInstance instance,
                                                                                         OperationService operationService) {
        int resultPartitionId = 0;
        for (; resultPartitionId < instance.getPartitionService().getPartitions().size(); resultPartitionId++) {
            if (resultPartitionId == givenPartitionId) {
                continue;
            }
            if (!getPartitionService(instance).getPartition(resultPartitionId).isLocal()) {
                continue;
            }
            if (mappedToSameThread(operationService, givenPartitionId, resultPartitionId)) {
                break;
            }
        }
        return resultPartitionId;
    }
}
