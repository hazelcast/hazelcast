/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.collection.operations.client.TxnSetAddRequest;
import com.hazelcast.collection.operations.client.TxnSetRemoveRequest;
import com.hazelcast.collection.operations.client.TxnSetSizeRequest;
import com.hazelcast.core.TransactionalSet;

/**
 * @ali 6/11/13
 */
public class ClientTxnSetProxy<E> extends ClientTxnProxy implements TransactionalSet<E> {

    public ClientTxnSetProxy(CollectionProxyId id, TransactionContextProxy proxy) {
        super(id, proxy);
    }

    public boolean add(E e) {
        TxnSetAddRequest request = new TxnSetAddRequest(getName(), toData(e));
        Boolean result = invoke(request);
        return result;
    }

    public boolean remove(E e) {
        TxnSetRemoveRequest request = new TxnSetRemoveRequest(getName(), toData(e));
        Boolean result = invoke(request);
        return result;
    }

    public int size() {
        TxnSetSizeRequest request = new TxnSetSizeRequest(getName());
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
