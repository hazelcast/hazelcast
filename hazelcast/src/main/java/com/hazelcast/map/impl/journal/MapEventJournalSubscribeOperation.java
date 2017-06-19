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

package com.hazelcast.map.impl.journal;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.journal.EventJournalInitialSubscriberState;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.spi.DistributedObjectNamespace;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.version.Version;

/**
 * Performs the initial subscription to the map event journal.
 * This includes retrieving the event journal sequences of the
 * oldest and newest event in the journal.
 *
 * @since 3.9
 */
public class MapEventJournalSubscribeOperation extends MapOperation implements PartitionAwareOperation, ReadonlyOperation {
    private EventJournalInitialSubscriberState response;
    private DistributedObjectNamespace namespace;

    public MapEventJournalSubscribeOperation() {
    }

    public MapEventJournalSubscribeOperation(String name) {
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
        namespace = new DistributedObjectNamespace(MapService.SERVICE_NAME, name);
        if (!mapServiceContext.getEventJournal().hasEventJournal(namespace)) {
            throw new UnsupportedOperationException(
                    "Cannot subscribe to event journal because it is either not configured or disabled for map " + name);
        }
    }

    @Override
    public void run() {
        final MapEventJournal eventJournal = mapServiceContext.getEventJournal();
        final long newestSequence = eventJournal.newestSequence(namespace, getPartitionId());
        final long oldestSequence = eventJournal.oldestSequence(namespace, getPartitionId());
        response = new EventJournalInitialSubscriberState(oldestSequence, newestSequence);
    }

    @Override
    public EventJournalInitialSubscriberState getResponse() {
        return response;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.EVENT_JOURNAL_SUBSCRIBE_OPERATION;
    }
}
