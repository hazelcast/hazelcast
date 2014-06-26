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

import com.hazelcast.multimap.MultiMapPortableHook;
import com.hazelcast.multimap.operations.RemoveOperation;
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

public class RemoveRequest extends MultiMapKeyBasedRequest {

    Data value;

    long threadId;

    public RemoveRequest() {
    }

    public RemoveRequest(String name, Data key, Data value, long threadId) {
        super(name, key);
        this.value = value;
        this.threadId = threadId;
    }

    protected Operation prepareOperation() {
        return new RemoveOperation(name, key, threadId, value);
    }

    public int getClassId() {
        return MultiMapPortableHook.REMOVE;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeLong("t", threadId);
        super.write(writer);
        final ObjectDataOutput out = writer.getRawDataOutput();
        value.writeData(out);
    }

    public void read(PortableReader reader) throws IOException {
        threadId = reader.readLong("t");
        super.read(reader);
        final ObjectDataInput in = reader.getRawDataInput();
        value = new Data();
        value.readData(in);
    }

    public Permission getRequiredPermission() {
        return new MultiMapPermission(name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getMethodName() {
        return "remove";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{key, value};
    }
}
