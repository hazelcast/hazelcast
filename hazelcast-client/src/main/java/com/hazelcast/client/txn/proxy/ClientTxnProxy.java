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

package com.hazelcast.client.txn.proxy;

import com.hazelcast.client.ClientDestroyRequest;
import com.hazelcast.client.ClientRequest;
import com.hazelcast.client.spi.impl.ClientInvocationServiceImpl;
import com.hazelcast.client.txn.BaseTransactionRequest;
import com.hazelcast.client.txn.TransactionContextProxy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;

/**
 * @author ali 6/10/13
 */
abstract class ClientTxnProxy implements TransactionalObject {

    final String objectName;
    final TransactionContextProxy proxy;

    ClientTxnProxy(String objectName, TransactionContextProxy proxy) {
        this.objectName = objectName;
        this.proxy = proxy;
    }

    final <T> T invoke(ClientRequest request) {
        if (request instanceof BaseTransactionRequest) {
            ((BaseTransactionRequest) request).setTxnId(proxy.getTxnId());
            ((BaseTransactionRequest) request).setClientThreadId(Thread.currentThread().getId());
        }
        final ClientInvocationServiceImpl invocationService = (ClientInvocationServiceImpl)
                proxy.getClient().getInvocationService();
        final SerializationService ss = proxy.getClient().getSerializationService();
        try {
            final Future f = invocationService.send(request, proxy.getConnection());
            return ss.toObject(f.get());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    abstract void onDestroy();

    public final void destroy() {
        onDestroy();
        ClientDestroyRequest request = new ClientDestroyRequest(objectName, getServiceName());
        invoke(request);
    }

    public Object getId() {
        return objectName;
    }

    public String getPartitionKey() {
        return StringPartitioningStrategy.getPartitionKey(getName());
    }

    Data toData(Object obj) {
        return proxy.getClient().getSerializationService().toData(obj);
    }

    Object toObject(Data data) {
        return proxy.getClient().getSerializationService().toObject(data);
    }
}
