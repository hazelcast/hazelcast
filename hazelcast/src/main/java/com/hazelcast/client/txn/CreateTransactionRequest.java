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
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionManagerService;
import com.hazelcast.transaction.TransactionOptions;

import java.io.IOException;
import java.security.Permission;

/**
 * @author ali 6/6/13
 */
public class CreateTransactionRequest extends BaseTransactionRequest implements SecureRequest {

    TransactionOptions options;

    public CreateTransactionRequest() {
    }

    public CreateTransactionRequest(TransactionOptions options) {
        this.options = options;
    }

    public Object innerCall() throws Exception {
        ClientEngineImpl clientEngine = getService();
        final ClientEndpoint endpoint = getEndpoint();
        final TransactionManagerService transactionManagerService = clientEngine.getTransactionManagerService();
        final TransactionContext context = transactionManagerService.newClientTransactionContext(options, endpoint.getUuid());
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
        options.writeData(writer.getRawDataOutput());
    }

    public void read(PortableReader reader) throws IOException {
        options = new TransactionOptions();
        options.readData(reader.getRawDataInput());
    }

    public Permission getRequiredPermission() {
        return new TransactionPermission();
    }
}
