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

package com.hazelcast.spi.impl;

import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.transaction.impl.Transaction;

/**
 * To centralize de-serialization for transactional proxies
 *
 * @param <S> type of the {@link RemoteService}
 */
public abstract class TransactionalDistributedObject<S extends RemoteService> extends AbstractDistributedObject<S> {

    protected final Transaction tx;

    protected TransactionalDistributedObject(NodeEngine nodeEngine, S service, Transaction tx) {
        super(nodeEngine, service);
        this.tx = tx;
    }

    protected Object toObjectIfNeeded(Object data) {
        if (tx.isOriginatedFromClient()) {
            return data;
        }
        return getNodeEngine().toObject(data);
    }
}
