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

package com.hazelcast.map.impl.querycache.event;

import com.hazelcast.map.impl.event.EventData;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.BinaryInterface;

import java.io.IOException;

/**
 * {@link EventData} which is used only for the subscriber end of a query cache
 * and only for query cache wide events like clearing all items together.
 *
 * For this reason, it is not sent over the wire and is used locally to query cache.
 *
 * Throws {@link UnsupportedOperationException} if one tries to serialize an instance of this class.
 */
@BinaryInterface
public class LocalCacheWideEventData implements EventData {

    private final String source;
    private final int eventType;
    private final int numberOfEntriesAffected;

    public LocalCacheWideEventData(String source, int eventType, int numberOfEntriesAffected) {
        this.source = source;
        this.eventType = eventType;
        this.numberOfEntriesAffected = numberOfEntriesAffected;
    }

    public int getNumberOfEntriesAffected() {
        return numberOfEntriesAffected;
    }

    @Override
    public String getSource() {
        return source;
    }

    @Override
    public String getMapName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Address getCaller() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getEventType() {
        return eventType;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "LocalCacheWideEventData{"
                + "eventType=" + eventType
                + ", source='" + source + '\''
                + ", numberOfEntriesAffected=" + numberOfEntriesAffected
                + '}';
    }
}
