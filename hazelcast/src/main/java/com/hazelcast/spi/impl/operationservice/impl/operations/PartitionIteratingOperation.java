/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationAccessor;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.OperationResponseHandler;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.PartitionTaskFactory;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.BitSet;
import java.util.Map;
import java.util.UUID;
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
    public CallStatus call() {
        return new OffloadImpl();
    }

    @Override
    public void onExecutionFailure(Throwable cause) {
        // we also send a response so that the caller doesn't wait indefinitely.
        sendResponse(new ErrorResponse(cause, getCallId(), isUrgent()));
        getLogger().severe(cause);
    }

    private OperationServiceImpl getOperationService() {
        return (OperationServiceImpl) getNodeEngine().getOperationService();
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
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

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", operationFactory=").append(operationFactory);
    }

    private final class OffloadImpl extends Offload {
        private OffloadImpl() {
            super(PartitionIteratingOperation.this);
        }

        @Override
        public void start() {
            if (partitions.length == 0) {
                // partitions may be empty if the node has joined and didn't get any partitions yet
                // a generic operation may already execute on it.
                sendResponse(EMPTY_RESPONSE);
                return;
            }

            PartitionAwareOperationFactory partitionAwareFactory = extractPartitionAware(operationFactory);
            if (partitionAwareFactory == null) {
                executeOperations();
            } else {
                executeOperations(partitionAwareFactory);
            }
        }

        private void executeOperations() {
            PartitionTaskFactory f = new PartitionTaskFactory() {
                private final NodeEngine nodeEngine = getNodeEngine();
                private final OperationResponseHandler responseHandler = new OperationResponseHandlerImpl(partitions);
                private final Object service = getServiceName() == null ? null : getService();

                @Override
                public Operation create(int partitionId) {
                    Operation op = operationFactory.createOperation()
                            .setNodeEngine(nodeEngine)
                            .setPartitionId(partitionId)
                            .setReplicaIndex(getReplicaIndex())
                            .setOperationResponseHandler(responseHandler)
                            .setServiceName(getServiceName())
                            .setService(service)
                            .setCallerUuid(extractCallerUuid());

                    OperationAccessor.setCallerAddress(op, getCallerAddress());
                    return op;
                }
            };

            getOperationService().executeOnPartitions(f, toPartitionBitSet());
        }

        private void executeOperations(PartitionAwareOperationFactory givenFactory) {
            final NodeEngine nodeEngine = getNodeEngine();
            final PartitionAwareOperationFactory factory = givenFactory.createFactoryOnRunner(nodeEngine, partitions);
            final OperationResponseHandler responseHandler = new OperationResponseHandlerImpl(partitions);
            final Object service = getServiceName() == null ? null : getService();

            PartitionTaskFactory f = partitionId -> {
                Operation op = factory.createPartitionOperation(partitionId)
                        .setNodeEngine(nodeEngine)
                        .setPartitionId(partitionId)
                        .setReplicaIndex(getReplicaIndex())
                        .setOperationResponseHandler(responseHandler)
                        .setServiceName(getServiceName())
                        .setService(service)
                        .setCallerUuid(extractCallerUuid());

                OperationAccessor.setCallerAddress(op, getCallerAddress());
                return op;
            };

            getOperationService().executeOnPartitions(f, toPartitionBitSet());
        }

        private BitSet toPartitionBitSet() {
            BitSet bitSet = new BitSet(getNodeEngine().getPartitionService().getPartitionCount());
            for (int partition : partitions) {
                bitSet.set(partition);
            }
            return bitSet;
        }

        private UUID extractCallerUuid() {
            // Clients callerUUID can be set already. See OperationFactoryWrapper usage.
            if (operationFactory instanceof OperationFactoryWrapper) {
                return ((OperationFactoryWrapper) operationFactory).getUuid();
            }

            // Members UUID
            return getCallerUuid();
        }
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
                sendResponse();
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

        @SuppressFBWarnings("EI_EXPOSE_REP")
        public int[] getPartitions() {
            return partitions;
        }

        @Override
        public int getFactoryId() {
            return SpiDataSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
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
