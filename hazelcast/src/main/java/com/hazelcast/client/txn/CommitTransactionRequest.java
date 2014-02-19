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
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionAccessor;

import java.io.IOException;

/**
 * @author ali 6/7/13
 */
public class CommitTransactionRequest extends BaseTransactionRequest implements Portable {

    boolean prepareAndCommit = false;

    public CommitTransactionRequest() {
    }

    public CommitTransactionRequest(boolean prepareAndCommit) {
        this.prepareAndCommit = prepareAndCommit;
    }

    public Object innerCall() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final TransactionContext transactionContext = endpoint.getTransactionContext(txnId);
        if (prepareAndCommit) {
            transactionContext.commitTransaction();
        } else {
            final Transaction transaction = TransactionAccessor.getTransaction(transactionContext);
            transaction.commit();
        }
        endpoint.removeTransactionContext(txnId);
        return null;
    }

    public String getServiceName() {
        return ClientEngineImpl.SERVICE_NAME;
    }

    public int getFactoryId() {
        return ClientTxnPortableHook.F_ID;
    }

    public int getClassId() {
        return ClientTxnPortableHook.COMMIT;
    }

    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeBoolean("pc", prepareAndCommit);
    }

    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        prepareAndCommit =  reader.readBoolean("pc");
    }
}
