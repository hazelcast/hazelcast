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

package com.hazelcast.cache.impl.journal;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.internal.journal.EventJournalInitialSubscriberState;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.impl.operationservice.AbstractNamedOperation;

/**
 * Performs the initial subscription to the cache event journal.
 * This includes retrieving the event journal sequences of the
 * oldest and newest event in the journal.
 *
 * @since 3.9
 */
public class CacheEventJournalSubscribeOperation
        extends AbstractNamedOperation
        implements PartitionAwareOperation, IdentifiedDataSerializable, ReadonlyOperation {
    private EventJournalInitialSubscriberState response;
    private ObjectNamespace namespace;

    public CacheEventJournalSubscribeOperation() {
    }

    public CacheEventJournalSubscribeOperation(String name) {
        super(name);
    }

    @Override
    public void beforeRun() throws Exception {
        super.beforeRun();

        namespace = CacheService.getObjectNamespace(name);
        final CacheService service = getService();
        if (!service.getEventJournal().hasEventJournal(namespace)) {
            throw new UnsupportedOperationException(
                    "Cannot subscribe to event journal because it is either not configured or disabled for cache " + name);
        }
    }

    @Override
    public void run() {
        final CacheService service = getService();
        final CacheEventJournal eventJournal = service.getEventJournal();
        final long newestSequence = eventJournal.newestSequence(namespace, getPartitionId());
        final long oldestSequence = eventJournal.oldestSequence(namespace, getPartitionId());
        response = new EventJournalInitialSubscriberState(oldestSequence, newestSequence);
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.EVENT_JOURNAL_SUBSCRIBE_OPERATION;
    }

    @Override
    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }


    @Override
    public EventJournalInitialSubscriberState getResponse() {
        return response;
    }
}
