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

import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionRecord;
import com.hazelcast.collection.CollectionWrapper;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.*;

/**
 * @ali 1/21/13
 */
public class CompareAndRemoveOperation extends CollectionBackupAwareOperation {

    List<Data> dataList;
    transient Set<Long> idSet;

    boolean retain;

    public CompareAndRemoveOperation() {
    }

    public CompareAndRemoveOperation(CollectionProxyId proxyId, Data dataKey, int threadId, List<Data> dataList, boolean retain) {
        super(proxyId, dataKey, threadId);
        this.dataList = dataList;
        this.retain = retain;
    }

    public void run() throws Exception {
        CollectionWrapper wrapper = getCollectionWrapper();
        if (wrapper == null){
            response = false;
            return;
        }
        idSet = new HashSet<Long>();
        List objList = dataList;
        if (!isBinary()){
            objList = new ArrayList(dataList.size());
            for (Data data: dataList){
                objList.add(toObject(data));
            }
        }
        Iterator<CollectionRecord> iter = wrapper.getCollection().iterator();
        while (iter.hasNext()) {
            CollectionRecord record = iter.next();
            boolean contains = objList.contains(record.getObject());
            if ((contains && !retain) || (!contains && retain)) {
                idSet.add(record.getRecordId());
                iter.remove();
            }
        }
        response = !idSet.isEmpty();
        if (wrapper.getCollection().isEmpty()){
            removeCollection();
        }
    }

    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    public Operation getBackupOperation() {
        return new CompareAndRemoveBackupOperation(proxyId, dataKey, idSet);
    }

    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(retain);
        out.writeInt(dataList.size());
        for (Data data : dataList) {
            data.writeData(out);
        }
    }

    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        retain = in.readBoolean();
        int size = in.readInt();
        dataList = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            Data data = IOUtil.readData(in);
            dataList.add(data);
        }
    }

    public int getId() {
        return CollectionDataSerializerHook.COMPARE_AND_REMOVE;
    }
}
