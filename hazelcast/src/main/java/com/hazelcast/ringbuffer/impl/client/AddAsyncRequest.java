/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.ringbuffer.impl.client;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.impl.operations.AddOperation;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;

public class AddAsyncRequest extends RingbufferRequest {

    private Data item;
    private OverflowPolicy overflowPolicy;

    public AddAsyncRequest() {
    }

    public AddAsyncRequest(String name, Data item, OverflowPolicy overflowPolicy) {
        super(name);
        this.item = item;
        this.overflowPolicy = overflowPolicy;
    }

    @Override
    protected Operation prepareOperation() {
        return new AddOperation(name, item, overflowPolicy);
    }

    @Override
    public int getClassId() {
        return RingbufferPortableHook.ADD_ASYNC;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeInt("o", overflowPolicy.getId());
        writer.getRawDataOutput().writeData(item);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        overflowPolicy = OverflowPolicy.getById(reader.readInt("o"));
        item = reader.getRawDataInput().readData();
    }
}
