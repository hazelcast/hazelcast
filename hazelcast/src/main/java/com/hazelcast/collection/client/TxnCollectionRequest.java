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

import com.hazelcast.client.client.SecureRequest;
import com.hazelcast.client.client.txn.BaseTransactionRequest;
import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public abstract class TxnCollectionRequest extends BaseTransactionRequest implements Portable, SecureRequest {

    String name;
    Data value;

    public TxnCollectionRequest() {
    }

    public TxnCollectionRequest(String name) {
        this.name = name;
    }

    public TxnCollectionRequest(String name, Data value) {
        this(name);
        this.value = value;
    }

    @Override
    public int getFactoryId() {
        return CollectionPortableHook.F_ID;
    }

    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeUTF("n", name);
        IOUtil.writeNullableData(writer.getRawDataOutput(), value);
    }

    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        name = reader.readUTF("n");
        value = IOUtil.readNullableData(reader.getRawDataInput());
    }
}

