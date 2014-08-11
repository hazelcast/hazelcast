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

package com.hazelcast.queue.impl.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.client.SecureRequest;
import com.hazelcast.client.client.txn.BaseTransactionRequest;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.queue.impl.QueuePortableHook;
import com.hazelcast.queue.impl.QueueService;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.QueuePermission;
import com.hazelcast.transaction.TransactionContext;

import java.io.IOException;
import java.security.Permission;
import java.util.concurrent.TimeUnit;

/**
 * Request for transactional poll operation.
 */
public class TxnPollRequest extends BaseTransactionRequest implements Portable, SecureRequest {

    private String name;
    private long timeout;

    public TxnPollRequest() {
    }

    public TxnPollRequest(String name, long timeout) {
        this.name = name;
        this.timeout = timeout;
    }

    @Override
    public Object innerCall() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final TransactionContext context = endpoint.getTransactionContext(txnId);
        final TransactionalQueue queue = context.getQueue(name);
        return queue.poll(timeout, TimeUnit.MILLISECONDS);
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
        return QueuePortableHook.TXN_POLL;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeUTF("n", name);
        writer.writeLong("t", timeout);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        name = reader.readUTF("n");
        timeout = reader.readLong("t");
    }

    @Override
    public Permission getRequiredPermission() {
        return new QueuePermission(name, ActionConstants.ACTION_REMOVE);
    }
}
