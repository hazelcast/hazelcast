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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.BooleanResultParameters;
import com.hazelcast.client.impl.protocol.parameters.IntResultParameters;
import com.hazelcast.client.spi.ClientTransactionContext;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.core.TransactionalList;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ThreadUtil;
import com.hazelcast.client.impl.protocol.parameters.TransactionalListAddParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalListRemoveParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalListSizeParameters;

public class ClientTxnListProxy<E> extends AbstractClientTxnCollectionProxy<E> implements TransactionalList<E> {

    public ClientTxnListProxy(String name, ClientTransactionContext transactionContext) {
        super(name, transactionContext);
    }

    public String getServiceName() {
        return ListService.SERVICE_NAME;
    }

    public boolean add(E e) {
        throwExceptionIfNull(e);
        final Data value = toData(e);
        ClientMessage request = TransactionalListAddParameters.encode(getName(), getTransactionId()
                , ThreadUtil.getThreadId(), value);
        ClientMessage response = invoke(request);
        return BooleanResultParameters.decode(response).result;
    }

    public boolean remove(E e) {
        throwExceptionIfNull(e);
        final Data value = toData(e);
        ClientMessage request = TransactionalListRemoveParameters.encode(getName(), getTransactionId()
                , ThreadUtil.getThreadId(), value);
        ClientMessage response = invoke(request);
        return BooleanResultParameters.decode(response).result;
    }

    public int size() {
        ClientMessage request = TransactionalListSizeParameters.encode(getName(), getTransactionId()
                , ThreadUtil.getThreadId());
        ClientMessage response = invoke(request);
        return IntResultParameters.decode(response).result;
    }

}
