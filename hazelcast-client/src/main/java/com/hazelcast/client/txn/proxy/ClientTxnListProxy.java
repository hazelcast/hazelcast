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

import com.hazelcast.client.txn.TransactionContextProxy;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.operations.client.TxnListAddRequest;
import com.hazelcast.collection.operations.client.TxnListRemoveRequest;
import com.hazelcast.collection.operations.client.TxnListSizeRequest;
import com.hazelcast.core.TransactionalList;

/**
 * @ali 6/11/13
 */
public class ClientTxnListProxy<E> extends ClientTxnProxy implements TransactionalList<E> {

    public ClientTxnListProxy(CollectionProxyId id, TransactionContextProxy proxy) {
        super(id, proxy);
    }

    public boolean add(E e) {
        TxnListAddRequest request = new TxnListAddRequest(getName(), toData(e));
        Boolean result = invoke(request);
        return result;
    }

    public boolean remove(E e) {
        TxnListRemoveRequest request = new TxnListRemoveRequest(getName(), toData(e));
        Boolean result = invoke(request);
        return result;
    }

    public int size() {
        TxnListSizeRequest request = new TxnListSizeRequest(getName());
        Integer result = invoke(request);
        return result;
    }

    public String getName() {
        final CollectionProxyId proxyId = (CollectionProxyId)getId();
        return proxyId.getKeyName();
    }

    void onDestroy() {
        //TODO
    }
}
