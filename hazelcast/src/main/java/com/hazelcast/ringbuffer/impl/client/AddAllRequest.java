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
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.ringbuffer.impl.operations.AddAllOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.RingBufferPermission;
import com.hazelcast.spi.Operation;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.security.Permission;
import java.util.Arrays;

public class AddAllRequest extends RingbufferRequest {

    private OverflowPolicy overflowPolicy;
    private Data[] items;

    public AddAllRequest() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public AddAllRequest(String name, Data[] items, OverflowPolicy overflowPolicy) {
        super(name);
        this.items = items;
        this.overflowPolicy = overflowPolicy;
    }

    //todo: async
    @Override
    protected Operation prepareOperation() {
        return new AddAllOperation(name, items, overflowPolicy);
    }

    @Override
    public int getClassId() {
        return RingbufferPortableHook.ADD_ALL;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeInt("o", overflowPolicy.getId());
        writer.writeInt("c", items.length);
        for (Data item : items) {
            writer.getRawDataOutput().writeData(item);
        }
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        overflowPolicy = OverflowPolicy.getById(reader.readInt("o"));

        int length = reader.readInt("c");
        items = new Data[length];
        for (int k = 0; k < items.length; k++) {
            items[k] = reader.getRawDataInput().readData();
        }
    }

    @Override
    public Permission getRequiredPermission() {
        return new RingBufferPermission(name, ActionConstants.ACTION_PUT);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{Arrays.asList(items), overflowPolicy};
    }

    @Override
    public String getMethodName() {
        return "addAllAsync";
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return RingbufferService.SERVICE_NAME;
    }
}
