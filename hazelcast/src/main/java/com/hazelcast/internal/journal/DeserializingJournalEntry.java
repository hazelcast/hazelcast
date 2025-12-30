/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.pipeline.JournalSourceEntry;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * A {@link DeserializingEntry} extension with a method to indicate lost entries
 * due to overlaps in the event journal.
 */
public class DeserializingJournalEntry<K, V> extends DeserializingEntry<K, V> implements JournalSourceEntry<K, V> {

    private boolean lostEventsDetected;

    public DeserializingJournalEntry(Data dataKey, Data dataValue, boolean lostEventDetected) {
        super(dataKey, dataValue);
        this.lostEventsDetected = lostEventDetected;
    }

    public DeserializingJournalEntry() {
    }

    @Override
    public int getClassId() {
        return EventJournalDataSerializerHook.DESERIALIZING_JOURNAL_ENTRY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeBoolean(lostEventsDetected);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        this.lostEventsDetected = in.readBoolean();
    }

    @Override
    public boolean isAfterLostEvents() {
        return lostEventsDetected;
    }
}
