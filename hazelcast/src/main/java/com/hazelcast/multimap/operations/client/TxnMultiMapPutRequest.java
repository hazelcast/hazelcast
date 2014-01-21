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

package com.hazelcast.multimap.operations.client;

import com.hazelcast.multimap.MultiMapPortableHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.transaction.TransactionContext;

import java.io.IOException;
import java.security.Permission;

/**
 * @author ali 6/10/13
 */
public class TxnMultiMapPutRequest extends TxnMultiMapRequest {

    Data key;
    Data value;

    public TxnMultiMapPutRequest() {
    }

    public TxnMultiMapPutRequest(String name, Data key, Data value, int clientThreadId) {
        super(name, clientThreadId);
        this.key = key;
        this.value = value;
    }

    public Object innerCall() throws Exception {
        final TransactionContext context = getEndpoint().getTransactionContext();
        return context.getMultiMap(name).put(key, value);
    }

    public int getClassId() {
        return MultiMapPortableHook.TXN_MM_PUT;
    }

    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        final ObjectDataOutput out = writer.getRawDataOutput();
        key.writeData(out);
        value.writeData(out);
    }

    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        final ObjectDataInput in = reader.getRawDataInput();
        key = new Data();
        key.readData(in);
        value = new Data();
        value.readData(in);
    }

    public Permission getRequiredPermission() {
        return new MultiMapPermission(name, ActionConstants.ACTION_PUT);
    }
}
