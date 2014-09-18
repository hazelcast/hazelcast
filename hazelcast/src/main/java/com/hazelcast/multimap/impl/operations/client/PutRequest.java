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

package com.hazelcast.multimap.impl.operations.client;

import com.hazelcast.multimap.impl.MultiMapPortableHook;
import com.hazelcast.multimap.impl.operations.PutOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.spi.Operation;
import java.io.IOException;
import java.security.Permission;

public class PutRequest extends MultiMapKeyBasedRequest {

    Data value;

    int index = -1;

    long threadId;

    public PutRequest() {
    }

    public PutRequest(String name, Data key, Data value, int index, long threadId) {
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

    public void write(PortableWriter writer) throws IOException {
        writer.writeInt("i", index);
        writer.writeLong("t", threadId);
        super.write(writer);
        final ObjectDataOutput out = writer.getRawDataOutput();
        value.writeData(out);
    }

    public void read(PortableReader reader) throws IOException {
        index = reader.readInt("i");
        threadId = reader.readLong("t");
        super.read(reader);
        final ObjectDataInput in = reader.getRawDataInput();
        value = new Data();
        value.readData(in);
    }

    public Permission getRequiredPermission() {
        return new MultiMapPermission(name, ActionConstants.ACTION_PUT);
    }

    @Override
    public String getMethodName() {
        return "put";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{key, value};
    }
}
