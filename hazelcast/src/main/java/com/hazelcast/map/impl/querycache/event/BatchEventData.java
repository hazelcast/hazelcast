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

package com.hazelcast.map.impl.querycache.event;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.map.impl.event.EventData;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static com.hazelcast.map.impl.querycache.event.QueryCacheEventDataBuilder.newQueryCacheEventDataBuilder;
import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Holder for a collection of {@link QueryCacheEventData}.
 * <p>
 * The {@link Versioned} interface is used due to {@link DefaultQueryCacheEventData}
 * requiring it, which is handled by the same {@link ObjectDataInput} as this class.
 * The {@link DefaultQueryCacheEventData} class should not have implemented
 * {@link Versioned} due to being annotated with {@link BinaryInterface}, so this
 * needs to be corrected in the next major release. For now, this class will implement
 * {@link Versioned} to avoid breaking serialization of the new field.
 *
 * @see QueryCacheEventData
 */
@BinaryInterface
public class BatchEventData implements Sequenced, EventData, Versioned {

    private String source;
    private Collection<QueryCacheEventData> events;
    private transient int partitionId;
    private transient String mapName;

    public BatchEventData() {
    }

    public BatchEventData(Collection<QueryCacheEventData> events, String source, int partitionId) {
        this.events = checkNotNull(events, "events cannot be null");
        this.source = checkNotNull(source, "source cannot be null");
        this.partitionId = checkNotNegative(partitionId, "partitionId cannot be negative");
    }

    public void add(QueryCacheEventData entry) {
        events.add(entry);
    }

    public Collection<QueryCacheEventData> getEvents() {
        return events;
    }

    public boolean isEmpty() {
        return events.isEmpty();
    }

    public int size() {
        return events.size();
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public String getSource() {
        return source;
    }

    @Override
    public String getMapName() {
        if (mapName != null) {
            return mapName;
        }
        if (events.isEmpty()) {
            return null;
        }
        // This batch should all relate to a single map
        QueryCacheEventData next = events.iterator().next();
        mapName = next.getMapName();
        return mapName;
    }

    @Override
    public Address getCaller() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getEventType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getSequence() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSequence(long sequence) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        Collection<QueryCacheEventData> events = this.events;
        out.writeString(source);
        out.writeInt(events.size());
        for (QueryCacheEventData eventData : events) {
            eventData.writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        source = in.readString();
        int size = in.readInt();
        if (size > 0) {
            events = new ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                QueryCacheEventData eventData = newQueryCacheEventDataBuilder(true).build();
                eventData.readData(in);
                assert in.getVersion().isLessThan(Versions.V5_4) || eventData.getMapName() != null : String
                        .format("Map name should not be null in version %s and above", Versions.V5_4);

                events.add(eventData);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BatchEventData that)) {
            return false;
        }

        return events != null ? events.equals(that.events) : that.events == null;
    }

    @Override
    public int hashCode() {
        return events != null ? events.hashCode() : 0;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("BatchEventData{");
        for (QueryCacheEventData event : events) {
            stringBuilder.append(event);
        }
        stringBuilder.append("}");
        return stringBuilder.toString();
    }
}
