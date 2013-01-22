/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.collection.CollectionProxyType;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @ali 1/21/13
 */
public class CompareAndRemoveBackupOperation extends CollectionKeyBasedOperation implements BackupOperation {

    List<Data> dataList;

    boolean retain;

    public CompareAndRemoveBackupOperation() {
    }

    public CompareAndRemoveBackupOperation(String name, CollectionProxyType proxyType, Data dataKey, List<Data> dataList, boolean retain) {
        super(name, proxyType, dataKey);
        this.dataList = dataList;
        this.retain = retain;
    }

    public void run() throws Exception {
        Collection coll = getOrCreateCollection();
        List list = dataList;
        if (!isBinary()){
            list = new ArrayList(dataList.size());
            for (Data data: dataList){
                list.add(toObject(data));
            }
        }

        if (retain){
            coll.retainAll(list);
        }
        else {
            coll.removeAll(list);
        }

        response = true;
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
}
