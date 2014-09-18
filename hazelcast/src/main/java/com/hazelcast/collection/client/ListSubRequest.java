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
import com.hazelcast.collection.list.ListSubOperation;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class ListSubRequest extends CollectionRequest {

    private int from;
    private int to;

    public ListSubRequest() {
    }

    public ListSubRequest(String name, int from, int to) {
        super(name);
        this.from = from;
        this.to = to;
    }

    @Override
    protected Operation prepareOperation() {
        return new ListSubOperation(name, from, to);
    }

    @Override
    public int getClassId() {
        return CollectionPortableHook.LIST_SUB;
    }

    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeInt("f", from);
        writer.writeInt("t", to);
    }

    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        from = reader.readInt("f");
        to = reader.readInt("t");
    }

    @Override
    public String getRequiredAction() {
        return ActionConstants.ACTION_READ;
    }

    @Override
    public String getMethodName() {
        if (from == -1 && to == -1) {
            return "listIterator";
        }
        return "subList";
    }

    @Override
    public Object[] getParameters() {
        if (from == -1 && to == -1) {
            return null;
        }
        return new Object[]{from, to};
    }
}
