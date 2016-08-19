/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.util.ResponseQueueFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import static com.hazelcast.util.CollectionUtil.toIntArray;

public final class PartitionIteratingOperation extends Operation implements IdentifiedDataSerializable {

    private OperationFactory operationFactory;
    private int[] partitions;
    private Object[] results;

    public PartitionIteratingOperation() {
    }

    public PartitionIteratingOperation(OperationFactory operationFactory, List<Integer> partitions) {
        this.operationFactory = operationFactory;
        this.partitions = toIntArray(partitions);
    }

    @Override
    public void run() throws Exception {
        try {
            Object[] responses;

            PartitionAwareOperationFactory partitionAware = getPartitionAwareFactoryOrNull();
            if (partitionAware != null) {
                responses = executePartitionAwareOperations(partitionAware);
            } else {
                responses = executeOperations();
            }

            results = resolveResponses(responses);
        } catch (Exception e) {
            getLogger(getNodeEngine()).severe(e);
        }
    }

    private PartitionAwareOperationFactory getPartitionAwareFactoryOrNull() {
        if (operationFactory instanceof PartitionAwareOperationFactory) {
            return ((PartitionAwareOperationFactory) operationFactory);
        }


        if (operationFactory instanceof OperationFactoryWrapper) {
            OperationFactory factory = ((OperationFactoryWrapper) operationFactory).getOperationFactory();
            if (factory instanceof PartitionAwareOperationFactory) {
                return ((PartitionAwareOperationFactory) factory);
            }
        }

        return null;
    }

    private Object[] executeOperations() {
        NodeEngine nodeEngine = getNodeEngine();
        Object[] responses = new Object[partitions.length];
        for (int i = 0; i < partitions.length; i++) {
            ResponseQueue responseQueue = new ResponseQueue();
            responses[i] = responseQueue;

            Operation operation = operationFactory.createOperation();
            operation.setNodeEngine(nodeEngine)
                    .setPartitionId(partitions[i])
                    .setReplicaIndex(getReplicaIndex())
                    .setOperationResponseHandler(responseQueue)
                    .setServiceName(getServiceName())
                    .setService(getService())
                    .setCallerUuid(extractCallerUuid());

            OperationAccessor.setCallerAddress(operation, getCallerAddress());
            nodeEngine.getOperationService().execute(operation);
        }
        return responses;
    }

    private String extractCallerUuid() {
        // Clients callerUUID can be set already. See OperationFactoryWrapper usage.
        if (operationFactory instanceof OperationFactoryWrapper) {
            return ((OperationFactoryWrapper) operationFactory).getUuid();
        }

        // Members UUID
        return getCallerUuid();
    }

    private Object[] executePartitionAwareOperations(PartitionAwareOperationFactory givenFactory) {
        PartitionAwareOperationFactory factory = givenFactory.createFactoryOnRunner(getNodeEngine());

        NodeEngine nodeEngine = getNodeEngine();
        int[] operationFactoryPartitions = factory.getPartitions();
        partitions = operationFactoryPartitions == null ? partitions : operationFactoryPartitions;
        Object[] responses = new Object[partitions.length];

        for (int i = 0; i < partitions.length; i++) {
            ResponseQueue responseQueue = new ResponseQueue();
            responses[i] = responseQueue;

            int partition = partitions[i];
            Operation operation = factory.createPartitionOperation(partition);

            operation.setNodeEngine(nodeEngine)
                    .setPartitionId(partition)
                    .setReplicaIndex(getReplicaIndex())
                    .setOperationResponseHandler(responseQueue)
                    .setServiceName(getServiceName())
                    .setService(getService())
                    .setCallerUuid(extractCallerUuid());

            OperationAccessor.setCallerAddress(operation, getCallerAddress());
            nodeEngine.getOperationService().execute(operation);
        }
        return responses;
    }

    /**
     * Replaces the {@link ResponseQueue} entries with its results.
     * <p>
     * The responses array is reused to avoid the allocation of a new array.
     */
    private Object[] resolveResponses(Object[] responses) throws InterruptedException {
        for (int i = 0; i < responses.length; i++) {
            ResponseQueue queue = (ResponseQueue) responses[i];
            Object result = queue.get();
            if (result instanceof NormalResponse) {
                responses[i] = ((NormalResponse) result).getValue();
            } else {
                responses[i] = result;
            }
        }
        return responses;
    }

    private ILogger getLogger(NodeEngine nodeEngine) {
        return nodeEngine.getLogger(PartitionIteratingOperation.class.getName());
    }

    @Override
    public Object getResponse() {
        return new PartitionResponse(partitions, results);
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", operationFactory=").append(operationFactory);
    }

    private static class ResponseQueue implements OperationResponseHandler {

        private final BlockingQueue<Object> queue = ResponseQueueFactory.newResponseQueue();

        @Override
        public void sendResponse(Operation op, Object obj) {
            if (!queue.offer(obj)) {
                throw new HazelcastException("Response could not be queued for transportation");
            }
        }

        public Object get() throws InterruptedException {
            return queue.take();
        }

        @Override
        public boolean isLocal() {
            return true;
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
            int resultLength = (results != null ? results.length : 0);
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
