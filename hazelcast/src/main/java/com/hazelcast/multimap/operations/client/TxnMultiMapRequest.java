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

import com.hazelcast.client.SecureRequest;
import com.hazelcast.client.txn.BaseTransactionRequest;
import com.hazelcast.multimap.MultiMapPortableHook;
import com.hazelcast.multimap.MultiMapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MultiMapPermission;

import java.io.IOException;
import java.security.Permission;

/**
 * @author ali 6/10/13
 */
public abstract class TxnMultiMapRequest extends BaseTransactionRequest implements SecureRequest {

    String name;

    protected TxnMultiMapRequest() {
    }

    protected TxnMultiMapRequest(String name) {
        this.name = name;
    }

    public String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return MultiMapPortableHook.F_ID;
    }

    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeUTF("n",name);
    }

    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        name = reader.readUTF("n");
    }

    public Data toData(Object obj){
        return getClientEngine().toData(obj);
    }

    public Permission getRequiredPermission() {
        return new MultiMapPermission(name, ActionConstants.ACTION_READ);
    }
}
