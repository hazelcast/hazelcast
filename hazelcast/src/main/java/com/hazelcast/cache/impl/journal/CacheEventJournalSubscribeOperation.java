/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.journal.EventJournalInitialSubscriberState;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.DistributedObjectNamespace;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;
import com.hazelcast.version.Version;

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
    private DistributedObjectNamespace namespace;

    public CacheEventJournalSubscribeOperation() {
    }

    public CacheEventJournalSubscribeOperation(String name) {
        super(name);
    }

    @Override
    public void beforeRun() throws Exception {
        super.beforeRun();

        final Version clusterVersion = getNodeEngine().getClusterService().getClusterVersion();
        if (clusterVersion.isLessThan(Versions.V3_9)) {
            throw new UnsupportedOperationException(
                    "Event journal actions are not available when cluster version is " + clusterVersion);
        }

        namespace = new DistributedObjectNamespace(CacheService.SERVICE_NAME, name);
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
    public int getId() {
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
