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

package com.hazelcast.collection.operations.client;

import com.hazelcast.client.InitializingObjectRequest;
import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.operations.CompareAndRemoveOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ali 5/10/13
 */
public class CompareAndRemoveRequest extends CollectionKeyBasedRequest implements InitializingObjectRequest {

    List<Data> dataList;

    boolean retain;

    int threadId;

    public CompareAndRemoveRequest() {
    }

    public CompareAndRemoveRequest(CollectionProxyId proxyId, Data key, List<Data> dataList, boolean retain, int threadId) {
        super(proxyId, key);
        this.dataList = dataList;
        this.retain = retain;
        this.threadId = threadId;
    }

    protected Operation prepareOperation() {
        return new CompareAndRemoveOperation(proxyId, key, threadId, dataList, retain);
    }

    public int getClassId() {
        return CollectionPortableHook.COMPARE_AND_REMOVE;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeBoolean("r",retain);
        writer.writeInt("t", threadId);
        writer.writeInt("s",dataList.size());
        final ObjectDataOutput out = writer.getRawDataOutput();
        for (Data data: dataList){
            data.writeData(out);
        }
        super.writePortable(writer);
    }

    public void readPortable(PortableReader reader) throws IOException {
        retain = reader.readBoolean("r");
        threadId = reader.readInt("t");
        int size = reader.readInt("s");
        final ObjectDataInput in = reader.getRawDataInput();
        dataList = new ArrayList<Data>(size);
        for (int i=0; i<size; i++){
            Data data = new Data();
            data.readData(in);
            dataList.add(data);
        }
        super.readPortable(reader);
    }
}
