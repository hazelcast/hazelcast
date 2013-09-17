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

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.InitializingObjectRequest;
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
 * @author ali 6/7/13
 */
public class TxnPollRequest extends CallableClientRequest implements Portable, InitializingObjectRequest {

    String name;
    long timeout;

    public TxnPollRequest() {
    }

    public TxnPollRequest(String name, long timeout) {
        this.name = name;
        this.timeout = timeout;
    }

    public Object call() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final TransactionContext context = endpoint.getTransactionContext();
        final TransactionalQueue queue = context.getQueue(name);
        return queue.poll(timeout, TimeUnit.MILLISECONDS);
    }

    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return QueuePortableHook.F_ID;
    }

    public int getClassId() {
        return QueuePortableHook.TXN_POLL;
    }

    public String getObjectName() {
        return name;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n",name);
        writer.writeLong("t",timeout);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        timeout = reader.readLong("t");
    }
}
