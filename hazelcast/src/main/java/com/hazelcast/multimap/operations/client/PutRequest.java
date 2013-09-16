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

package com.hazelcast.multimap.operations.client;

import com.hazelcast.client.InitializingObjectRequest;
import com.hazelcast.multimap.MultiMapPortableHook;
import com.hazelcast.multimap.operations.PutOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @author ali 5/10/13
 */
public class PutRequest extends MultiMapKeyBasedRequest implements InitializingObjectRequest {

    Data value;

    int index = -1;

    int threadId = -1;

    public PutRequest() {
    }

    public PutRequest(String name, Data key, Data value, int index, int threadId) {
        super(name, key);
        this.value = value;
        this.index = index;
        this.threadId = threadId;
    }

    protected Operation prepareOperation() {
        return new PutOperation(name, key, threadId, value, index);
    }

    public int getClassId() {
        return MultiMapPortableHook.PUT;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("i",index);
        writer.writeInt("t", threadId);
        super.writePortable(writer);
        final ObjectDataOutput out = writer.getRawDataOutput();
        value.writeData(out);
    }

    public void readPortable(PortableReader reader) throws IOException {
        index = reader.readInt("i");
        threadId = reader.readInt("t");
        super.readPortable(reader);
        final ObjectDataInput in = reader.getRawDataInput();
        value = new Data();
        value.readData(in);
    }
}
