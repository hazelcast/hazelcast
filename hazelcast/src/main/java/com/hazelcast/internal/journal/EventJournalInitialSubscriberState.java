/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.journal;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * The response for the event journal subcription. This includes
 * the sequence IDs of the newest and oldest event in the event
 * journal from which new events can be read.
 * Keep in mind that these sequence IDs may be overwritten at any point
 * and may be stale by the time you decide to read them.
 *
 * @see com.hazelcast.map.impl.journal.MapEventJournalSubscribeOperation
 * @see com.hazelcast.cache.impl.journal.CacheEventJournalSubscribeOperation
 * @since 3.9
 */
public class EventJournalInitialSubscriberState implements IdentifiedDataSerializable {
    private long oldestSequence;
    private long newestSequence;

    public EventJournalInitialSubscriberState() {
    }

    public EventJournalInitialSubscriberState(long oldestSequence, long newestSequence) {
        this.oldestSequence = oldestSequence;
        this.newestSequence = newestSequence;
    }

    public long getOldestSequence() {
        return oldestSequence;
    }

    public long getNewestSequence() {
        return newestSequence;
    }

    @Override
    public int getFactoryId() {
        return EventJournalDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EventJournalDataSerializerHook.EVENT_JOURNAL_INITIAL_SUBSCRIBER_STATE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(oldestSequence);
        out.writeLong(newestSequence);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        oldestSequence = in.readLong();
        newestSequence = in.readLong();
    }
}
