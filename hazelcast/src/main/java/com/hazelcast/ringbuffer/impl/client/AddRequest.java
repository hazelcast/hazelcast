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
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.RingBufferPermission;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;

public class AddRequest extends RingbufferRequest {

    private Data item;

    public AddRequest() {
    }

    public AddRequest(String name, Data item) {
        super(name);
        this.item = item;
    }

    @Override
    protected Operation prepareOperation() {
        return new AddOperation(name, item, OverflowPolicy.FAIL);
    }

    @Override
    public int getClassId() {
        return RingbufferPortableHook.ADD;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.getRawDataOutput().writeData(item);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        item = reader.getRawDataInput().readData();
    }
    @Override
    public Permission getRequiredPermission() {
        return new RingBufferPermission(name, ActionConstants.ACTION_PUT);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{item};
    }

    @Override
    public String getMethodName() {
        return "add";
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }
}
