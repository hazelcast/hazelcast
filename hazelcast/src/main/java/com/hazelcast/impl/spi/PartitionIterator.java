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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static com.hazelcast.nio.IOUtil.toObject;

public class PartitionIterator extends AbstractOperation {
    ArrayList<Integer> partitions;
    Data callableOperation;

    public PartitionIterator(ArrayList<Integer> partitions, Data callableOperation) {
        this.partitions = partitions;
        this.callableOperation = callableOperation;
    }

    public PartitionIterator() {
    }

    public Response call() throws Exception {
        final NodeService nodeService = getOperationContext().getNodeService();
        final Object service = getOperationContext().getService();
        Map<Integer, Object> results = new HashMap<Integer, Object>(partitions.size());
        Map<Integer, Future> responses = new HashMap<Integer, Future>(partitions.size());
        for (final int partitionId : partitions) {
            Future f = nodeService.runLocally(partitionId, new Callable() {
                public Object call() throws Exception {
                    if (!nodeService.isOwner(partitionId)) {
                        return "NOT OWNER";
                    } else {
                        Operation mpe = (Operation) toObject(callableOperation);
                        mpe.getOperationContext().setPartitionId(partitionId).setService(service);
                        return mpe.call();
                    }
                }
            }, false);
            responses.put(partitionId, f);
        }
        for (Map.Entry<Integer, Future> partitionResponse : responses.entrySet()) {
            results.put(partitionResponse.getKey(), partitionResponse.getValue().get());
        }
        return new Response(results);
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        super.writeData(out);
        int pCount = partitions.size();
        out.writeInt(pCount);
        for (int i = 0; i < pCount; i++) {
            out.writeInt(partitions.get(i));
        }
        callableOperation.writeData(out);
    }

    @Override
    public void readData(DataInput in) throws IOException {
        super.readData(in);
        int pCount = in.readInt();
        partitions = new ArrayList<Integer>(pCount);
        for (int i = 0; i < pCount; i++) {
            partitions.add(in.readInt());
        }
        callableOperation = new Data();
        callableOperation.readData(in);
    }
}
