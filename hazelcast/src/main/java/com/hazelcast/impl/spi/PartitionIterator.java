/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.spi;

import com.hazelcast.nio.Data;
import com.hazelcast.util.ResponseQueueFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import static com.hazelcast.nio.IOUtil.toObject;

class PartitionIterator extends AbstractOperation {
    ArrayList<Integer> partitions;
    Data operationData;

    public PartitionIterator(ArrayList<Integer> partitions, Data operationData) {
        this.partitions = partitions;
        this.operationData = operationData;
    }

    public PartitionIterator() {
    }

    public void run() {
        OperationContext context = getOperationContext();
        try {
            final NodeService nodeService = context.getNodeService();
            Map<Integer, Object> results = new HashMap<Integer, Object>(partitions.size());
            Map<Integer, ResponseQueue> responses = new HashMap<Integer, ResponseQueue>(partitions.size());
            for (final int partitionId : partitions) {
                Operation op = (Operation) toObject(operationData);
                op.getOperationContext().setNodeService(getOperationContext().getNodeService())
                        .setCaller(getOperationContext().getCaller())
                        .setPartitionId(partitionId)
                        .setLocal(true)
                        .setService(getOperationContext().getService());
                final ResponseQueue responseQueue = new ResponseQueue();
                op.getOperationContext().setResponseHandler(responseQueue);
                nodeService.runLocally(partitionId, op, false);
                responses.put(partitionId, responseQueue);
            }
            for (Map.Entry<Integer, ResponseQueue> partitionResponse : responses.entrySet()) {
                Object result = partitionResponse.getValue().get();
                results.put(partitionResponse.getKey(), result);
            }
            context.getResponseHandler().sendResponse(results);
        } catch (Exception e) {
            e.printStackTrace();
            context.getResponseHandler().sendResponse(e);
        }
    }

    class ResponseQueue implements ResponseHandler {
        final BlockingQueue b = ResponseQueueFactory.newResponseQueue();

        public void sendResponse(Object obj) {
            b.offer(obj);
        }

        public Object get() throws InterruptedException {
            return b.take();
        }
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        super.writeData(out);
        int pCount = partitions.size();
        out.writeInt(pCount);
        for (int i = 0; i < pCount; i++) {
            out.writeInt(partitions.get(i));
        }
        operationData.writeData(out);
    }

    @Override
    public void readData(DataInput in) throws IOException {
        super.readData(in);
        int pCount = in.readInt();
        partitions = new ArrayList<Integer>(pCount);
        for (int i = 0; i < pCount; i++) {
            partitions.add(in.readInt());
        }
        operationData = new Data();
        operationData.readData(in);
    }
}
