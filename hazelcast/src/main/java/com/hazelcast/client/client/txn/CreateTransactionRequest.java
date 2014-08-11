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

package com.hazelcast.client.client.txn;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.client.SecureRequest;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.impl.SerializableXID;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionAccessor;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;

import java.io.IOException;
import java.security.Permission;

public class CreateTransactionRequest extends BaseTransactionRequest implements SecureRequest {

    private TransactionOptions options;
    private SerializableXID sXid;

    public CreateTransactionRequest() {
    }

    public CreateTransactionRequest(TransactionOptions options, SerializableXID sXid) {
        this.options = options;
        this.sXid = sXid;
    }

    @Override
    public Object innerCall() throws Exception {
        ClientEngineImpl clientEngine = getService();
        ClientEndpoint endpoint = getEndpoint();
        TransactionManagerServiceImpl transactionManager =
                (TransactionManagerServiceImpl) clientEngine.getTransactionManagerService();
        TransactionContext context = transactionManager.newClientTransactionContext(options, endpoint.getUuid());
        if (sXid != null) {
            Transaction transaction = TransactionAccessor.getTransaction(context);
            transactionManager.addManagedTransaction(sXid, transaction);
        }
        context.beginTransaction();
        endpoint.setTransactionContext(context);
        return context.getTxnId();
    }

    @Override
    public String getServiceName() {
        return ClientEngineImpl.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ClientTxnPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClientTxnPortableHook.CREATE;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        ObjectDataOutput out = writer.getRawDataOutput();
        options.writeData(out);
        out.writeBoolean(sXid != null);
        if (sXid != null) {
            sXid.writeData(out);
        }
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        ObjectDataInput in = reader.getRawDataInput();
        options = new TransactionOptions();
        options.readData(in);
        boolean sXidNotNull = in.readBoolean();
        if (sXidNotNull) {
            sXid = new SerializableXID();
            sXid.readData(in);
        }

    }

    @Override
    public Permission getRequiredPermission() {
        return new TransactionPermission();
    }
}
