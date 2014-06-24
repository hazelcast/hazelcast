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

package com.hazelcast.map.client;

import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.operation.ReplaceIfSameOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class MapReplaceIfSameRequest extends MapPutRequest {

    private Data testValue;

    public MapReplaceIfSameRequest() {
    }

    public MapReplaceIfSameRequest(String name, Data key, Data testValue, Data value, long threadId) {
        super(name, key, value, threadId);
        this.testValue = testValue;
    }

    public int getClassId() {
        return MapPortableHook.REPLACE_IF_SAME;
    }

    protected Operation prepareOperation() {
        ReplaceIfSameOperation op = new ReplaceIfSameOperation(name, key, testValue, value);
        op.setThreadId(threadId);
        return op;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        final ObjectDataOutput out = writer.getRawDataOutput();
        testValue.writeData(out);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        final ObjectDataInput in = reader.getRawDataInput();
        testValue = new Data();
        testValue.readData(in);
    }

    @Override
    public String getMethodName() {
        return "replace";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{key, testValue, value};
    }
}
