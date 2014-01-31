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

package com.hazelcast.collection.txn;

import com.hazelcast.collection.CollectionItem;
import com.hazelcast.collection.set.SetService;
import com.hazelcast.core.TransactionalSet;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.impl.TransactionSupport;

import java.util.Collection;
import java.util.HashSet;

/**
* @author ali 4/16/13
*/
public class TransactionalSetProxy<E> extends AbstractTransactionalCollectionProxy<SetService, E>
        implements TransactionalSet<E> {

    private final HashSet<CollectionItem> set = new HashSet<CollectionItem>();

    public TransactionalSetProxy(String name, TransactionSupport tx, NodeEngine nodeEngine, SetService service) {
        super(name, tx, nodeEngine, service);
    }

    @Override
    public String getServiceName() {
        return SetService.SERVICE_NAME;
    }

    @Override
    protected Collection<CollectionItem> getCollection() {
        return set;
    }
}
