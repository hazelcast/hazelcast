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

package com.hazelcast.client.txn;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngineImpl;
import com.hazelcast.client.SecureRequest;
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

/**
 * @author ali 6/6/13
 */
public class CreateTransactionRequest extends BaseTransactionRequest implements SecureRequest {

    TransactionOptions options;

    SerializableXID sXid;

    public CreateTransactionRequest() {
    }

    public CreateTransactionRequest(TransactionOptions options, SerializableXID sXid) {
        this.options = options;
        this.sXid = sXid;
    }

    public Object innerCall() throws Exception {
        ClientEngineImpl clientEngine = getService();
        final ClientEndpoint endpoint = getEndpoint();
        final TransactionManagerServiceImpl transactionManager =
                (TransactionManagerServiceImpl)clientEngine.getTransactionManagerService();
        final TransactionContext context = transactionManager.newClientTransactionContext(options, endpoint.getUuid());
        if (sXid != null) {
            final Transaction transaction = TransactionAccessor.getTransaction(context);
            transactionManager.addManagedTransaction(sXid, transaction);
        }
        context.beginTransaction();
        endpoint.setTransactionContext(context);
        return context.getTxnId();
    }

    public String getServiceName() {
        return ClientEngineImpl.SERVICE_NAME;
    }

    public int getFactoryId() {
        return ClientTxnPortableHook.F_ID;
    }

    public int getClassId() {
        return ClientTxnPortableHook.CREATE;
    }

    public void write(PortableWriter writer) throws IOException {
        final ObjectDataOutput out = writer.getRawDataOutput();
        options.writeData(out);
        out.writeObject(sXid);
    }

    public void read(PortableReader reader) throws IOException {
        options = new TransactionOptions();
        final ObjectDataInput in = reader.getRawDataInput();
        options.readData(in);
        sXid = in.readObject();

    }

    public Permission getRequiredPermission() {
        return new TransactionPermission();
    }
}
