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

package com.hazelcast.client.txn.proxy;

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.ClientDestroyRequest;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.spi.ClientTransactionContext;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.client.BaseTransactionRequest;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;

abstract class ClientTxnProxy implements TransactionalObject {

    final String objectName;
    final ClientTransactionContext transactionContext;

    ClientTxnProxy(String objectName, ClientTransactionContext transactionContext) {
        this.objectName = objectName;
        this.transactionContext = transactionContext;
    }

    final <T> T invoke(ClientRequest request) {
        if (request instanceof BaseTransactionRequest) {
            ((BaseTransactionRequest) request).setTxnId(transactionContext.getTxnId());
            ((BaseTransactionRequest) request).setClientThreadId(Thread.currentThread().getId());
        }
        HazelcastClientInstanceImpl client = transactionContext.getClient();
        SerializationService ss = client.getSerializationService();
        try {
            ClientInvocation invocation = new ClientInvocation(client, request, transactionContext.getConnection());
            Future<SerializableCollection> future = invocation.invoke();
            return ss.toObject(future.get());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    abstract void onDestroy();

    @Override
    public final void destroy() {
        onDestroy();
        ClientDestroyRequest request = new ClientDestroyRequest(objectName, getServiceName());
        invoke(request);
    }

    @Override
    public Object getId() {
        return objectName;
    }

    @Override
    public String getPartitionKey() {
        return StringPartitioningStrategy.getPartitionKey(getName());
    }

    Data toData(Object obj) {
        return transactionContext.getClient().getSerializationService().toData(obj);
    }

    Object toObject(Data data) {
        return transactionContext.getClient().getSerializationService().toObject(data);
    }
}
