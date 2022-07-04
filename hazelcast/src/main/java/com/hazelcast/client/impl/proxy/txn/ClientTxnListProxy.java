/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.proxy.txn;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.TransactionalListAddCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalListRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalListSizeCodec;
import com.hazelcast.client.impl.spi.ClientTransactionContext;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.transaction.TransactionalList;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.Preconditions;

import static com.hazelcast.internal.util.ThreadUtil.getThreadId;

/**
 * Proxy implementation of {@link TransactionalList}.
 *
 * @param <E> the type of elements in this list
 */
public class ClientTxnListProxy<E> extends AbstractClientTxnCollectionProxy implements TransactionalList<E> {

    public ClientTxnListProxy(String name, ClientTransactionContext transactionContext) {
        super(name, transactionContext);
    }

    @Override
    public String getServiceName() {
        return ListService.SERVICE_NAME;
    }

    @Override
    public boolean add(E e) {
        Preconditions.checkNotNull(e);
        Data value = toData(e);
        ClientMessage request = TransactionalListAddCodec.encodeRequest(name, getTransactionId(), getThreadId(), value);
        ClientMessage response = invoke(request);
        return TransactionalListAddCodec.decodeResponse(response);
    }

    @Override
    public boolean remove(E e) {
        Preconditions.checkNotNull(e);
        Data value = toData(e);
        ClientMessage request = TransactionalListRemoveCodec.encodeRequest(name, getTransactionId(), getThreadId(), value);
        ClientMessage response = invoke(request);
        return TransactionalListRemoveCodec.decodeResponse(response);
    }

    @Override
    public int size() {
        ClientMessage request = TransactionalListSizeCodec.encodeRequest(name, getTransactionId(), getThreadId());
        ClientMessage response = invoke(request);
        return TransactionalListSizeCodec.decodeResponse(response);
    }
}
