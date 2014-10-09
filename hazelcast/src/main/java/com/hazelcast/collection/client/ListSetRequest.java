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

package com.hazelcast.collection.client;

import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.collection.list.ListSetOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class ListSetRequest extends CollectionRequest {

    private int index;

    private Data value;

    public ListSetRequest() {
    }

    public ListSetRequest(String name, int index, Data value) {
        super(name);
        this.index = index;
        this.value = value;
    }

    @Override
    protected Operation prepareOperation() {
        return new ListSetOperation(name, index, value);
    }

    @Override
    public int getClassId() {
        return CollectionPortableHook.LIST_SET;
    }

    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeInt("i", index);
        writer.getRawDataOutput().writeData(value);
    }

    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        index = reader.readInt("i");
        value = reader.getRawDataInput().readData();
    }

    @Override
    public String getRequiredAction() {
        return ActionConstants.ACTION_READ;
    }

    @Override
    public String getMethodName() {
        return "set";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{index, value};
    }
}
