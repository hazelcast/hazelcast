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
import com.hazelcast.collection.CollectionRemoveOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class CollectionRemoveRequest extends CollectionRequest {

    private Data value;

    public CollectionRemoveRequest() {
    }

    public CollectionRemoveRequest(String name, Data value) {
        super(name);
        this.value = value;
    }

    @Override
    protected Operation prepareOperation() {
        return new CollectionRemoveOperation(name, value);
    }

    @Override
    public int getClassId() {
        return CollectionPortableHook.COLLECTION_REMOVE;
    }

    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        value.writeData(writer.getRawDataOutput());
    }

    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        value = new Data();
        value.readData(reader.getRawDataInput());
    }

    @Override
    public String getRequiredAction() {
        return ActionConstants.ACTION_REMOVE;
    }

    @Override
    public String getMethodName() {
        return "remove";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{value};
    }
}
