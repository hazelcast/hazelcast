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

package com.hazelcast.collection.impl.txnqueue.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.transaction.client.BaseTransactionRequest;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.collection.impl.queue.QueuePortableHook;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.QueuePermission;
import com.hazelcast.transaction.TransactionContext;

import java.io.IOException;
import java.security.Permission;

/**
 * Request for size of the Transactional Queue.
 */
public class TxnSizeRequest extends BaseTransactionRequest implements Portable, SecureRequest {

    String name;

    public TxnSizeRequest() {
    }

    public TxnSizeRequest(String name) {
        this.name = name;
    }

    @Override
    public Object innerCall() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final TransactionContext context = endpoint.getTransactionContext(txnId);
        final TransactionalQueue queue = context.getQueue(name);
        return queue.size();
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
        return QueuePortableHook.TXN_SIZE;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeUTF("n", name);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        name = reader.readUTF("n");
    }

    @Override
    public Permission getRequiredPermission() {
        return new QueuePermission(name, ActionConstants.ACTION_READ);
    }
}
