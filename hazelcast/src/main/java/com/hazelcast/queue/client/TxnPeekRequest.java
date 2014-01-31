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

package com.hazelcast.queue.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.txn.BaseTransactionRequest;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.queue.QueuePortableHook;
import com.hazelcast.queue.QueueService;
import com.hazelcast.transaction.TransactionContext;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * User: ahmetmircik
 * Date: 10/1/13
 * Time: 10:34 AM
 */
public class TxnPeekRequest extends BaseTransactionRequest implements Portable {

    private String name;

    private long timeout;

    public TxnPeekRequest(){
    }

    public TxnPeekRequest(String name, long timeout) {
        this.name = name;
        this.timeout = timeout;
    }

    @Override
    public Object innerCall() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final TransactionContext context = endpoint.getTransactionContext(txnId);
        final TransactionalQueue queue = context.getQueue(name);
        return queue.peek(timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return QueuePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return QueuePortableHook.TXN_PEEK;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeUTF("n",name);
        writer.writeLong("t",timeout);
    }

    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        name = reader.readUTF("n");
        timeout = reader.readLong("t");
    }
}
