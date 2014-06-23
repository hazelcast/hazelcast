/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.util.ResponseQueueFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public final class PartitionIteratingOperation extends AbstractOperation implements IdentifiedDataSerializable {

    private List<Integer> partitions;
    private OperationFactory operationFactory;
    private Map<Integer, Object> results;

    public PartitionIteratingOperation(List<Integer> partitions, OperationFactory operationFactory) {
        this.partitions = partitions != null ? partitions : Collections.<Integer>emptyList();
        this.operationFactory = operationFactory;
    }

    public PartitionIteratingOperation() {
    }

    public void run() throws Exception {
        final NodeEngine nodeEngine = getNodeEngine();
        results = new HashMap<Integer, Object>(partitions.size());
        try {
            Map<Integer, ResponseQueue> responses = new HashMap<Integer, ResponseQueue>(partitions.size());
            for (final int partitionId : partitions) {
                ResponseQueue responseQueue = new ResponseQueue();
                final Operation op = operationFactory.createOperation();
                op.setNodeEngine(nodeEngine)
                        .setPartitionId(partitionId)
                        .setReplicaIndex(getReplicaIndex())
                        .setResponseHandler(responseQueue)
                        .setServiceName(getServiceName())
                        .setService(getService())
                        .setCallerUuid(getCallerUuid());
                OperationAccessor.setCallerAddress(op, getCallerAddress());
                responses.put(partitionId, responseQueue);
                nodeEngine.getOperationService().executeOperation(op);
            }
            for (Map.Entry<Integer, ResponseQueue> responseQueueEntry : responses.entrySet()) {
                final ResponseQueue queue = responseQueueEntry.getValue();
                final Integer key = responseQueueEntry.getKey();
                final Object result = queue.get();
                if (result instanceof NormalResponse) {
                    results.put(key, ((NormalResponse) result).getValue());
                } else {
                    results.put(key, result);
                }
            }
        } catch (Exception e) {
            getLogger(nodeEngine).severe(e);
        }
    }

    @Override
    public void afterRun() throws Exception {
    }

    private ILogger getLogger(NodeEngine nodeEngine) {
        return nodeEngine.getLogger(PartitionIteratingOperation.class.getName());
    }

    @Override
    public Object getResponse() {
        return new PartitionResponse(results);
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    private static class ResponseQueue implements ResponseHandler {
        final BlockingQueue b = ResponseQueueFactory.newResponseQueue();

        @Override
        public void sendResponse(Object obj) {
            b.offer(obj);
        }

        public Object get() throws InterruptedException {
            return b.take();
        }

        @Override
        public boolean isLocal() {
            return true;
        }
    }

    // To make serialization of HashMap faster.
    public static final class PartitionResponse implements IdentifiedDataSerializable {

        private Map<Integer, Object> results;

        public PartitionResponse() {
        }

        public PartitionResponse(Map<Integer, Object> results) {
            this.results = results != null ? results : Collections.<Integer, Object>emptyMap();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            int len = results != null ? results.size() : 0;
            out.writeInt(len);
            if (len > 0) {
                for (Map.Entry<Integer, Object> entry : results.entrySet()) {
                    out.writeInt(entry.getKey());
                    out.writeObject(entry.getValue());
                }
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            int len = in.readInt();
            if (len > 0) {
                results = new HashMap<Integer, Object>(len);
                for (int i = 0; i < len; i++) {
                    int pid = in.readInt();
                    Object value = in.readObject();
                    results.put(pid, value);
                }
            } else {
                results = Collections.emptyMap();
            }
        }

        public Map<? extends Integer, ?> asMap() {
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
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        int pCount = partitions.size();
        out.writeInt(pCount);
        for (Integer partition : partitions) {
            out.writeInt(partition);
        }
        out.writeObject(operationFactory);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int pCount = in.readInt();
        partitions = new ArrayList<Integer>(pCount);
        for (int i = 0; i < pCount; i++) {
            partitions.add(in.readInt());
        }
        operationFactory = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SpiDataSerializerHook.PARTITION_ITERATOR;
    }
}
