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
import com.hazelcast.collection.client.TxnSetAddRequest;
import com.hazelcast.collection.client.TxnSetRemoveRequest;
import com.hazelcast.collection.client.TxnSetSizeRequest;
import com.hazelcast.collection.set.SetService;
import com.hazelcast.core.TransactionalSet;
import com.hazelcast.nio.serialization.Data;

/**
* @author ali 6/11/13
*/
public class ClientTxnSetProxy<E> extends AbstractClientTxnCollectionProxy<E> implements TransactionalSet<E> {

    public ClientTxnSetProxy(String name, TransactionContextProxy proxy) {
        super(name, proxy);
    }

    public boolean add(E e) {
        throwExceptionIfNull(e);
        final Data value = toData(e);
        final TxnSetAddRequest request = new TxnSetAddRequest(getName(), value);
        final Boolean result = invoke(request);
        return result;
    }

    public boolean remove(E e) {
        throwExceptionIfNull(e);
        final Data value = toData(e);
        final TxnSetRemoveRequest request = new TxnSetRemoveRequest(getName(), value);
        final Boolean result = invoke(request);
        return result;
    }

    public int size() {
        final TxnSetSizeRequest request = new TxnSetSizeRequest(getName());
        final Integer result = invoke(request);
        return result;
    }

    public String getServiceName() {
        return SetService.SERVICE_NAME;
    }

}
