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

import com.hazelcast.collection.CollectionAddOperation;
import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @ali 9/4/13
 */
public class CollectionAddRequest extends CollectionRequest {

    protected Data value;

    public CollectionAddRequest() {
    }

    public CollectionAddRequest(String name, Data value) {
        super(name);
        this.value = value;
    }

    protected Operation prepareOperation() {
        return new CollectionAddOperation(name, value);
    }

    public int getClassId() {
        return CollectionPortableHook.COLLECTION_ADD;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        super.writePortable(writer);
        value.writeData(writer.getRawDataOutput());
    }

    public void readPortable(PortableReader reader) throws IOException {
        super.readPortable(reader);
        value = new Data();
        value.readData(reader.getRawDataInput());
    }
}
