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

package com.hazelcast.client.proxy.txn;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.TransactionalSetAddCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalSetRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalSetSizeCodec;
import com.hazelcast.client.spi.ClientTransactionContext;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.core.TransactionalSet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ThreadUtil;

public class ClientTxnSetProxy<E> extends AbstractClientTxnCollectionProxy<E> implements TransactionalSet<E> {

    public ClientTxnSetProxy(String name, ClientTransactionContext transactionContext) {
        super(name, transactionContext);
    }

    public boolean add(E e) {
        throwExceptionIfNull(e);
        Data value = toData(e);
        ClientMessage request = TransactionalSetAddCodec.encodeRequest(getName(), getTransactionId()
                , ThreadUtil.getThreadId(), value);
        ClientMessage response = invoke(request);
        return TransactionalSetAddCodec.decodeResponse(response).response;
    }

    public boolean remove(E e) {
        throwExceptionIfNull(e);
        Data value = toData(e);
        ClientMessage request = TransactionalSetRemoveCodec.encodeRequest(getName(), getTransactionId()
                , ThreadUtil.getThreadId(), value);
        ClientMessage response = invoke(request);
        return TransactionalSetRemoveCodec.decodeResponse(response).response;
    }

    public int size() {
        ClientMessage request = TransactionalSetSizeCodec.encodeRequest(getName(), getTransactionId()
                , ThreadUtil.getThreadId());
        ClientMessage response = invoke(request);
        return TransactionalSetSizeCodec.decodeResponse(response).response;
    }

    public String getServiceName() {
        return SetService.SERVICE_NAME;
    }

}
