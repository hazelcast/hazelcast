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

package com.hazelcast.transaction.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionAccessor;

import java.io.IOException;
import java.security.Permission;

public class CommitTransactionRequest extends BaseTransactionRequest {

    private boolean prepareAndCommit;

    public CommitTransactionRequest() {
    }

    public CommitTransactionRequest(boolean prepareAndCommit) {
        this.prepareAndCommit = prepareAndCommit;
    }

    @Override
    public Object innerCall() throws Exception {
        ClientEndpoint endpoint = getEndpoint();
        TransactionContext transactionContext = endpoint.getTransactionContext(txnId);
        if (prepareAndCommit) {
            transactionContext.commitTransaction();
        } else {
            Transaction transaction = TransactionAccessor.getTransaction(transactionContext);
            transaction.commit();
        }
        endpoint.removeTransactionContext(txnId);
        return null;
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
        return ClientTxnPortableHook.COMMIT;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeBoolean("pc", prepareAndCommit);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        prepareAndCommit = reader.readBoolean("pc");
    }

    @Override
    public Permission getRequiredPermission() {
        return new TransactionPermission();
    }
}
