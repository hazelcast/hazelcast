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

import com.hazelcast.client.impl.client.CallableClientRequest;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.xa.TransactionAccessor;
import com.hazelcast.transaction.impl.xa.XAService;

import java.io.IOException;
import java.security.Permission;

public abstract class BaseXATransactionRequest extends CallableClientRequest {

    protected String txnId;

    public BaseXATransactionRequest() {
    }

    public BaseXATransactionRequest(String txnId) {
        this.txnId = txnId;
    }

    protected Transaction getTransaction() {
        TransactionContext transactionContext = endpoint.getTransactionContext(txnId);
        if (transactionContext == null) {
            throw new TransactionException("No transaction context with given transactionId: " + txnId);
        }
        return TransactionAccessor.getTransaction(transactionContext);
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("tId", txnId);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        txnId = reader.readUTF("tId");
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
    public Permission getRequiredPermission() {
        return new TransactionPermission();
    }
}
