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

package com.hazelcast.queue.client;

import com.hazelcast.client.RetryableRequest;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.queue.ContainsOperation;
import com.hazelcast.queue.QueuePortableHook;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author ali 5/8/13
 */
public class ContainsRequest extends QueueRequest implements RetryableRequest {

    Collection<Data> dataList;

    public ContainsRequest() {
    }

    public ContainsRequest(String name, Collection<Data> dataList) {
        super(name);
        this.dataList = dataList;
    }

    protected Operation prepareOperation() {
        return new ContainsOperation(name, dataList);
    }

    public int getClassId() {
        return QueuePortableHook.CONTAINS;
    }

    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeInt("s", dataList.size());
        final ObjectDataOutput out = writer.getRawDataOutput();
        for (Data data : dataList) {
            data.writeData(out);
        }
    }

    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        int size = reader.readInt("s");
        final ObjectDataInput in = reader.getRawDataInput();
        dataList = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            Data data = new Data();
            data.readData(in);
            dataList.add(data);
        }
    }
}
