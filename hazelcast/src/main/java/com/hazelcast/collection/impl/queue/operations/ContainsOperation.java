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

package com.hazelcast.collection.impl.queue.operations;

import com.hazelcast.collection.impl.queue.QueueContainer;
import com.hazelcast.collection.impl.queue.QueueDataSerializerHook;
import com.hazelcast.internal.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Checks whether contain or not item in the Queue.
 */
public class ContainsOperation extends QueueOperation implements ReadonlyOperation {

    private Collection<Data> dataList;

    public ContainsOperation() {
    }

    public ContainsOperation(String name, Collection<Data> dataList) {
        super(name);
        this.dataList = dataList;
    }

    @Override
    public void run() throws Exception {
        QueueContainer queueContainer = getContainer();
        response = queueContainer.contains(dataList);
    }

    @Override
    public void afterRun() throws Exception {
        LocalQueueStatsImpl stats = getQueueService().getLocalQueueStatsImpl(name);
        stats.incrementOtherOperations();
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.CONTAINS;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(dataList.size());
        for (Data data : dataList) {
            IOUtil.writeData(out, data);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        dataList = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            dataList.add(IOUtil.readData(in));
        }
    }
}
