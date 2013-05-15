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

package com.hazelcast.collection.operations;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionRecord;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @ali 1/17/13
 */
public class AddAllOperation extends CollectionBackupAwareOperation {

    List<Data> dataList;
    transient Collection<CollectionRecord> recordList;

    int index = -1;

    public AddAllOperation() {
    }

    public AddAllOperation(CollectionProxyId proxyId, Data dataKey, int threadId, List<Data> dataList, int index) {
        super(proxyId, dataKey, threadId);
        this.dataList = dataList;
        this.index = index;
    }

    public void run() throws Exception {
        CollectionContainer container = getOrCreateContainer();
        Collection<CollectionRecord> coll = container.getOrCreateCollectionWrapper(dataKey).getCollection();
        recordList = new ArrayList<CollectionRecord>(dataList.size());
        try {
            int i = 0;
            for (Data data : dataList) {
                CollectionRecord record = new CollectionRecord(isBinary() ? data : toObject(data));
                boolean added = true;
                if (index == -1) {
                    added = coll.add(record);
                } else {
                    List list = (List) coll;
                    list.add(index+(i++), record);
                }
                if (added) {
                    record.setRecordId(container.nextId());
                    recordList.add(record);
                }
            }
            response = recordList.size() != 0;
        } catch (IndexOutOfBoundsException e) {
            response = e;
        }
    }

    public Operation getBackupOperation() {
        return new AddAllBackupOperation(proxyId, dataKey, recordList, index);
    }

    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(index);
        out.writeInt(dataList.size());
        for (Data data : dataList) {
            data.writeData(out);
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        index = in.readInt();
        int size = in.readInt();
        dataList = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            Data data = IOUtil.readData(in);
            dataList.add(data);
        }
    }

    public int getId() {
        return CollectionDataSerializerHook.ADD_ALL;
    }
}
