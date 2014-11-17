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

package com.hazelcast.multimap.impl.client;

import com.hazelcast.core.TransactionalMultiMap;
import com.hazelcast.multimap.impl.MultiMapPortableHook;
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

public class TxnMultiMapRemoveRequest extends TxnMultiMapRequest {

    Data key;
    Data value;

    public TxnMultiMapRemoveRequest() {
    }

    public TxnMultiMapRemoveRequest(String name, Data key) {
        super(name);
        this.key = key;
    }

    public TxnMultiMapRemoveRequest(String name, Data key, Data value) {
        this(name, key);
        this.value = value;
    }

    public Object innerCall() throws Exception {
        final TransactionContext context = getEndpoint().getTransactionContext(txnId);
        final TransactionalMultiMap<Object, Object> multiMap = context.getMultiMap(name);
        return multiMap.remove(key, value);
    }

    public int getClassId() {
        return MultiMapPortableHook.TXN_MM_REMOVE;
    }

    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeData(key);
        out.writeData(value);
    }

    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        final ObjectDataInput in = reader.getRawDataInput();
        key = in.readData();
        value = in.readData();
    }

    public Permission getRequiredPermission() {
        return new MultiMapPermission(name, ActionConstants.ACTION_REMOVE);
    }
}
