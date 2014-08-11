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

package com.hazelcast.client.client.txn.proxy;

import com.hazelcast.client.client.txn.TransactionContextProxy;
import com.hazelcast.collection.client.TxnListAddRequest;
import com.hazelcast.collection.client.TxnListRemoveRequest;
import com.hazelcast.collection.client.TxnListSizeRequest;
import com.hazelcast.collection.list.ListService;
import com.hazelcast.core.TransactionalList;
import com.hazelcast.nio.serialization.Data;

/**
* @author ali 6/11/13
*/
public class ClientTxnListProxy<E> extends AbstractClientTxnCollectionProxy<E> implements TransactionalList<E> {

    public ClientTxnListProxy(String name, TransactionContextProxy proxy) {
        super(name, proxy);
    }

    public String getServiceName() {
        return ListService.SERVICE_NAME;
    }

    public boolean add(E e) {
        throwExceptionIfNull(e);
        final Data value = toData(e);
        final TxnListAddRequest request = new TxnListAddRequest(getName(), value);
        final Boolean result = invoke(request);
        return result;
    }

    public boolean remove(E e) {
        throwExceptionIfNull(e);
        final Data value = toData(e);
        final TxnListRemoveRequest request = new TxnListRemoveRequest(getName(), value);
        final Boolean result = invoke(request);
        return result;
    }

    public int size() {
        final TxnListSizeRequest request = new TxnListSizeRequest(getName());
        final Integer result = invoke(request);
        return result;
    }

}
