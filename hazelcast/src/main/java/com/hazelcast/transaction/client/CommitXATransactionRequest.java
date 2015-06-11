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

import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.transaction.impl.Transaction;

import java.io.IOException;

public class CommitXATransactionRequest extends BaseXATransactionRequest {

    private boolean onePhase;

    public CommitXATransactionRequest() {
    }

    public CommitXATransactionRequest(String txnId, boolean onePhase) {
        super(txnId);
        this.onePhase = onePhase;
    }

    @Override
    public Object call() throws Exception {
        Transaction transaction = getTransaction();
        if (onePhase) {
            transaction.prepare();
        }
        transaction.commit();
        endpoint.removeTransactionContext(txnId);
        return null;
    }

    @Override
    public int getClassId() {
        return ClientTxnPortableHook.COMMIT_XA;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeBoolean("o", onePhase);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        onePhase = reader.readBoolean("o");
    }
}
