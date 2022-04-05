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

package com.hazelcast.map.impl;

import com.hazelcast.map.impl.tx.TransactionalMapProxy;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.services.TransactionalService;
import com.hazelcast.transaction.impl.Transaction;

import java.util.UUID;

/**
 * Defines transactional service behavior of map service.
 *
 * @see MapService
 */
class MapTransactionalService implements TransactionalService {

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;

    MapTransactionalService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
    }

    @SuppressWarnings("unchecked")
    @Override
    public TransactionalMapProxy createTransactionalObject(String name, Transaction transaction) {
        return new TransactionalMapProxy(name, mapServiceContext.getService(), nodeEngine, transaction);
    }

    @Override
    public void rollbackTransaction(UUID transactionId) {
    }
}
