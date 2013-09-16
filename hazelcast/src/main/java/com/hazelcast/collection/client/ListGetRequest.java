/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.collection.list.ListGetOperation;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @ali 9/4/13
 */
public class ListGetRequest extends CollectionRequest{

    int index = -1;

    public ListGetRequest() {
    }

    public ListGetRequest(String name, int index) {
        super(name);
        this.index = index;
    }

    protected Operation prepareOperation() {
        return new ListGetOperation(name, index);
    }

    public int getClassId() {
        return CollectionPortableHook.LIST_GET ;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        super.writePortable(writer);
        writer.writeInt("i", index);
    }

    public void readPortable(PortableReader reader) throws IOException {
        super.readPortable(reader);
        index = reader.readInt("i");
    }
}
