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

package com.hazelcast.collection.impl.txnlist;

import com.hazelcast.collection.impl.collection.CollectionItem;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.txncollection.AbstractTransactionalCollectionProxy;
import com.hazelcast.transaction.TransactionalList;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.transaction.impl.Transaction;

import java.util.Collection;
import java.util.LinkedList;

public class TransactionalListProxy<E>
        extends AbstractTransactionalCollectionProxy<ListService, E>
        implements TransactionalList<E> {

    private final LinkedList<CollectionItem> list = new LinkedList<CollectionItem>();

    public TransactionalListProxy(String name, Transaction tx, NodeEngine nodeEngine, ListService service) {
        super(name, tx, nodeEngine, service);
    }

    @Override
    public String getServiceName() {
        return ListService.SERVICE_NAME;
    }

    @Override
    protected Collection<CollectionItem> getCollection() {
        return list;
    }
}
