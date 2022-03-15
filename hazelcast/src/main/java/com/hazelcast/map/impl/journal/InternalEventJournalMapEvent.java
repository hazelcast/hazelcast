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

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * The event journal item for map events. It contains serialized
 * values for key and all values included in map mutations as well as
 * the event type.
 */
public class InternalEventJournalMapEvent implements IdentifiedDataSerializable {
    protected Data dataKey;
    protected Data dataNewValue;
    protected Data dataOldValue;
    protected int eventType;

    public InternalEventJournalMapEvent() {
    }

    public InternalEventJournalMapEvent(Data dataKey, Data dataNewValue, Data dataOldValue, int eventType) {
        this.eventType = eventType;
        this.dataKey = dataKey;
        this.dataNewValue = dataNewValue;
        this.dataOldValue = dataOldValue;
    }

    public Data getDataKey() {
        return dataKey;
    }

    public Data getDataNewValue() {
        return dataNewValue;
    }

    public Data getDataOldValue() {
        return dataOldValue;
    }

    /**
     * Return the integer defining the event type. It can be turned into an
     * enum value by calling {@link com.hazelcast.core.EntryEventType#getByType(int)}.
     *
     * @return the integer defining the event type
     */
    public int getEventType() {
        return eventType;
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.EVENT_JOURNAL_INTERNAL_MAP_EVENT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(eventType);
        IOUtil.writeData(out, dataKey);
        IOUtil.writeData(out, dataNewValue);
        IOUtil.writeData(out, dataOldValue);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        eventType = in.readInt();
        dataKey = IOUtil.readData(in);
        dataNewValue = IOUtil.readData(in);
        dataOldValue = IOUtil.readData(in);
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InternalEventJournalMapEvent that = (InternalEventJournalMapEvent) o;

        if (eventType != that.eventType) {
            return false;
        }
        if (dataKey != null ? !dataKey.equals(that.dataKey) : that.dataKey != null) {
            return false;
        }
        if (dataNewValue != null ? !dataNewValue.equals(that.dataNewValue) : that.dataNewValue != null) {
            return false;
        }
        return dataOldValue != null ? dataOldValue.equals(that.dataOldValue) : that.dataOldValue == null;
    }

    @Override
    public int hashCode() {
        int result = dataKey != null ? dataKey.hashCode() : 0;
        result = 31 * result + (dataNewValue != null ? dataNewValue.hashCode() : 0);
        result = 31 * result + (dataOldValue != null ? dataOldValue.hashCode() : 0);
        result = 31 * result + eventType;
        return result;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{eventType=" + eventType + '}';
    }
}
