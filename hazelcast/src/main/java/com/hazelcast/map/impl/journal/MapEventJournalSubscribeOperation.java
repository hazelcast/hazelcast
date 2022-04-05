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

package com.hazelcast.map.impl.journal;

import com.hazelcast.internal.journal.EventJournalInitialSubscriberState;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;

/**
 * Performs the initial subscription to the map event journal.
 * This includes retrieving the event journal sequences of the
 * oldest and newest event in the journal.
 *
 * @since 3.9
 */
public class MapEventJournalSubscribeOperation extends MapOperation
        implements PartitionAwareOperation, ReadonlyOperation {

    private EventJournalInitialSubscriberState response;
    private ObjectNamespace namespace;

    public MapEventJournalSubscribeOperation() {
    }

    public MapEventJournalSubscribeOperation(String name) {
        super(name);
    }

    @Override
    protected void innerBeforeRun() throws Exception {
        super.innerBeforeRun();

        namespace = getServiceNamespace();
        if (!mapServiceContext.getEventJournal().hasEventJournal(namespace)) {
            throw new UnsupportedOperationException(
                    "Cannot subscribe to event journal because it is either not configured or disabled for map '" + name + '\'');
        }
    }

    @Override
    protected void runInternal() {
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
    public int getClassId() {
        return MapDataSerializerHook.EVENT_JOURNAL_SUBSCRIBE_OPERATION;
    }
}
