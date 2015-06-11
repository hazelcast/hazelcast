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

package com.hazelcast.transaction.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.CallableClientRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.impl.xa.SerializableXID;
import com.hazelcast.transaction.impl.xa.TransactionAccessor;
import com.hazelcast.transaction.impl.xa.XAService;

import java.io.IOException;
import java.security.Permission;

public class CreateXATransactionRequest extends CallableClientRequest implements SecureRequest {

    SerializableXID xid;

    int timeout;

    public CreateXATransactionRequest() {
    }

    public CreateXATransactionRequest(SerializableXID xid, int timeout) {
        this.xid = xid;
        this.timeout = timeout;
    }

    @Override
    public Object call() throws Exception {
        ClientEndpoint endpoint = getEndpoint();
        XAService xaService = getService();
        TransactionContext context = xaService.newXATransactionContext(xid, timeout);
        TransactionAccessor.getTransaction(context).begin();
        endpoint.setTransactionContext(context);
        return context.getTxnId();
    }

    @Override
    public String getServiceName() {
        return XAService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ClientTxnPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClientTxnPortableHook.CREATE_XA;
    }

    @Override
    public Permission getRequiredPermission() {
        return new TransactionPermission();
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeInt("t", timeout);
        ObjectDataOutput rawDataOutput = writer.getRawDataOutput();
        rawDataOutput.writeObject(xid);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        timeout = reader.readInt("t");
        ObjectDataInput rawDataInput = reader.getRawDataInput();
        xid = rawDataInput.readObject();
    }
}
