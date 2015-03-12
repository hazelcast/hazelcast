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

package com.hazelcast.collection.impl.list.client;

import com.hazelcast.collection.impl.collection.client.CollectionAddRequest;
import com.hazelcast.collection.impl.collection.CollectionPortableHook;
import com.hazelcast.collection.impl.list.operations.ListAddOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class ListAddRequest extends CollectionAddRequest {

    private int index;

    public ListAddRequest() {
    }

    public ListAddRequest(String name, Data value, int index) {
        super(name, value);
        this.index = index;
    }

    @Override
    protected Operation prepareOperation() {
        return new ListAddOperation(name, index, value);
    }

    @Override
    public int getClassId() {
        return CollectionPortableHook.LIST_ADD;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeInt("i", index);
        super.write(writer);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        index = reader.readInt("i");
        super.read(reader);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{index, value};
    }
}
