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

package com.hazelcast.collection.multimap.tx;

import com.hazelcast.collection.CollectionService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.Transaction;
import com.hazelcast.transaction.TransactionalObject;

/**
 * @ali 3/29/13
 */
public abstract class TransactionalMultiMapProxySupport extends AbstractDistributedObject<CollectionService> implements TransactionalObject{

    protected final String name;
    protected final Transaction tx;

    protected TransactionalMultiMapProxySupport(NodeEngine nodeEngine, CollectionService service, String name, Transaction tx) {
        super(nodeEngine, service);
        this.name = name;
        this.tx = tx;
    }

    public boolean putInternal(Data key, Data value){
        throwExceptionIfNull(key);
        throwExceptionIfNull(value);

        return false;
    }


    public Object getId() {
        return name;
    }

    public String getName() {
        return name;
    }

    public final String getServiceName() {
        return CollectionService.SERVICE_NAME;
    }

    private void throwExceptionIfNull(Object o){
        if (o == null){
            throw new NullPointerException("Object is null");
        }
    }
}
