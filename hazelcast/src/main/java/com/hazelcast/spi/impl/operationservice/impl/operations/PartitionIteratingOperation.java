/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.impl.operationservice.impl.operations;

import com.hazelcast.client.impl.operations.OperationFactoryWrapper;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareFactoryAccessor.extractPartitionAware;

/**
 * Executes Operations on one or more partitions.
 * <p>
 * THe execution used to be synchronous; so the thread executing this PartitionIteratingOperation would block till all
 * the child requests are done. In 3.8 this is made asynchronous so that the thread isn't consumed and available for
 * other tasks. On each partition operation an {@link OperationResponseHandler} is set, that sends the result to the
 * caller when all responses have completed.
 */
public final class PartitionIteratingOperation extends Operation implements IdentifiedDataSerializable {

    private static final Object NULL = new Object() {
        @Override
        public String toString() {
            return "null";
        }
    };

    private static final PartitionResponse EMPTY_RESPONSE = new PartitionResponse(new int[0], new Object[0]);

    private OperationFactory operationFactory;
    private int[] partitions;

    public PartitionIteratingOperation() {
    }

    /**
     * @param operationFactory operation factory to use
     * @param partitions       partitions to invoke on
     */
    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public PartitionIteratingOperation(OperationFactory operationFactory, int[] partitions) {
        this.operationFactory = operationFactory;
        this.partitions = partitions;
    }

    public OperationFactory getOperationFactory() {
        return operationFactory;
    }

    @Override
    public boolean returnsResponse() {
        // since this call is non blocking, we don't have a response. The response is send when the actual operations complete.
        return false;
    }

    @Override
    public void run() throws Exception {
        // partitions may be empty if the node has joined and didn't get any partitions yet
        // a generic operation may already execute on it.
        if (partitions.length == 0) {
            this.sendResponse(EMPTY_RESPONSE);
            return;
        }

        getOperationServiceImpl().onStartAsyncOperation(this);
        PartitionAwareOperationFactory partitionAwareFactory = extractPartitionAware(operationFactory);
        if (partitionAwareFactory != null) {
            executePartitionAwareOperations(partitionAwareFactory);
        } else {
            executeOperations();
        }
    }

    @Override
    public void onExecutionFailure(Throwable cause) {
        try {
            // we also send a response so that the caller doesn't wait indefinitely.
            sendResponse(new ErrorResponse(cause, getCallId(), isUrgent()));
        } finally {
            // in case of an error, we need to de-register to prevent leaks.
            getOperationServiceImpl().onCompletionAsyncOperation(this);
        }
        getLogger().severe(cause);
    }

    private void executeOperations() {
        NodeEngine nodeEngine = getNodeEngine();
        OperationResponseHandler responseHandler = new OperationResponseHandlerImpl(partitions);
        OperationService operationService = nodeEngine.getOperationService();
        Object service = getServiceName() == null ? null : getService();

        for (int partitionId : partitions) {
            Operation operation = operationFactory.createOperation()
                    .setNodeEngine(nodeEngine)
                    .setPartitionId(partitionId)
                    .setReplicaIndex(getReplicaIndex())
                    .setOperationResponseHandler(responseHandler)
                    .setServiceName(getServiceName())
                    .setService(service)
                    .setCallerUuid(extractCallerUuid());

            OperationAccessor.setCallerAddress(operation, getCallerAddress());
            operationService.execute(operation);
        }
    }

    private void executePartitionAwareOperations(PartitionAwareOperationFactory givenFactory) {
        PartitionAwareOperationFactory factory = givenFactory.createFactoryOnRunner(getNodeEngine());

        NodeEngine nodeEngine = getNodeEngine();
        int[] operationFactoryPartitions = factory.getPartitions();
        partitions = operationFactoryPartitions == null ? partitions : operationFactoryPartitions;

        OperationResponseHandler responseHandler = new OperationResponseHandlerImpl(partitions);
        OperationService operationService = nodeEngine.getOperationService();
        Object service = getServiceName() == null ? null : getService();

        for (int partitionId : partitions) {
            Operation op = factory.createPartitionOperation(partitionId)
                    .setNodeEngine(nodeEngine)
                    .setPartitionId(partitionId)
                    .setReplicaIndex(getReplicaIndex())
                    .setOperationResponseHandler(responseHandler)
                    .setServiceName(getServiceName())
                    .setService(service)
                    .setCallerUuid(extractCallerUuid());

            OperationAccessor.setCallerAddress(op, getCallerAddress());
            operationService.execute(op);
        }
    }

    private OperationServiceImpl getOperationServiceImpl() {
        return (OperationServiceImpl) getNodeEngine().getOperationService();
    }

    private String extractCallerUuid() {
        // Clients callerUUID can be set already. See OperationFactoryWrapper usage.
        if (operationFactory instanceof OperationFactoryWrapper) {
            return ((OperationFactoryWrapper) operationFactory).getUuid();
        }

        // Members UUID
        return getCallerUuid();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", operationFactory=").append(operationFactory);
    }

    private class OperationResponseHandlerImpl implements OperationResponseHandler {

        // an array with the partitionCount as length, so we can quickly do a lookup for a given partitionId.
        // it will store all the 'sub' responses.
        private final AtomicReferenceArray<Object> responseArray = new AtomicReferenceArray<Object>(
                getNodeEngine().getPartitionService().getPartitionCount());

        // contains the number of pending operations. If it hits zero, all responses have been received.
        private final AtomicInteger pendingOperations;
        private final int[] partitions;

        OperationResponseHandlerImpl(int[] partitions) {
            this.partitions = partitions;
            this.pendingOperations = new AtomicInteger(partitions.length);
        }

        @Override
        public void sendResponse(Operation op, Object response) {
            if (response instanceof NormalResponse) {
                response = ((NormalResponse) response).getValue();
            } else if (response == null) {
                response = NULL;
            }

            // we try to set the response in the responseArray
            if (!responseArray.compareAndSet(op.getPartitionId(), null, response)) {
                // duplicate response; should not happen. We can only log it since this method is being executed on some
                // general purpose executor.
                getLogger().warning("Duplicate response for " + op + " second response [" + response + "]"
                        + "first response [" + responseArray.get(op.getPartitionId()) + "]");
                return;
            }

            // if it is the last response we are waiting for, we can send the final response to the caller.
            if (pendingOperations.decrementAndGet() == 0) {
                try {
                    sendResponse();
                } finally {
                    getOperationServiceImpl().onCompletionAsyncOperation(PartitionIteratingOperation.this);
                }
            }
        }

        private void sendResponse() {
            Object[] results = new Object[partitions.length];
            for (int k = 0; k < partitions.length; k++) {
                int partitionId = partitions[k];
                Object response = responseArray.get(partitionId);
                results[k] = response == NULL ? null : response;
            }

            PartitionIteratingOperation.this.sendResponse(new PartitionResponse(partitions, results));
        }
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SpiDataSerializerHook.PARTITION_ITERATOR;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeObject(operationFactory);
        out.writeIntArray(partitions);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        operationFactory = in.readObject();
        partitions = in.readIntArray();
    }

    // implements IdentifiedDataSerializable to speed up serialization of arrays
    public static final class PartitionResponse implements IdentifiedDataSerializable {
        private int[] partitions;
        private Object[] results;

        public PartitionResponse() {
        }

        PartitionResponse(int[] partitions, Object[] results) {
            this.partitions = partitions;
            this.results = results;
        }

        public void addResults(Map<Integer, Object> partitionResults) {
            if (results == null) {
                return;
            }
            for (int i = 0; i < results.length; i++) {
                partitionResults.put(partitions[i], results[i]);
            }
        }

        @SuppressFBWarnings("EI_EXPOSE_REP")
        public Object[] getResults() {
            return results;
        }

        @Override
        public int getFactoryId() {
            return SpiDataSerializerHook.F_ID;
        }

        @Override
        public int getId() {
            return SpiDataSerializerHook.PARTITION_RESPONSE;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeIntArray(partitions);
            int resultLength = results != null ? results.length : 0;
            out.writeInt(resultLength);
            if (resultLength > 0) {
                for (Object result : results) {
                    out.writeObject(result);
                }
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            partitions = in.readIntArray();
            int resultLength = in.readInt();
            if (resultLength > 0) {
                results = new Object[resultLength];
                for (int i = 0; i < resultLength; i++) {
                    results[i] = in.readObject();
                }
            }
        }
    }
}
