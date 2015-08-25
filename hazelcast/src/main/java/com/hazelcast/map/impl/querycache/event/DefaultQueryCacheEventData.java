/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.io.IOException;

/**
 * Default implementation of {@link QueryCacheEventData} which is sent to subscriber.
 */
public class DefaultQueryCacheEventData
        implements QueryCacheEventData {

    private Object key;
    private Object value;
    private Data dataKey;
    private Data dataNewValue;
    private Data dataOldValue;
    private long sequence;
    private SerializationService serializationService;
    private final long creationTime;
    private int eventType;
    private int partitionId;

    public DefaultQueryCacheEventData() {
        creationTime = Clock.currentTimeMillis();
    }

    @Override
    public Object getKey() {
        if (key == null && dataKey != null) {
            key = serializationService.toObject(dataKey);
        }
        return key;
    }

    @Override
    public Object getValue() {
        if (value == null && dataNewValue != null) {
            value = serializationService.toObject(dataNewValue);
        }
        return value;
    }

    @Override
    public Data getDataKey() {
        return dataKey;
    }

    @Override
    public Data getDataNewValue() {
        return dataNewValue;
    }

    @Override
    public Data getDataOldValue() {
        return dataOldValue;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getSequence() {
        return sequence;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public int getEventType() {
        return eventType;
    }

    @Override
    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public void setDataKey(Data dataKey) {
        this.dataKey = dataKey;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public void setDataNewValue(Data dataNewValue) {
        this.dataNewValue = dataNewValue;
    }

    public void setDataOldValue(Data dataOldValue) {
        this.dataOldValue = dataOldValue;
    }

    public void setEventType(int eventType) {
        this.eventType = eventType;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public void setSerializationService(InternalSerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public String getSource() {
        throw new UnsupportedOperationException();
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
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(sequence);
        out.writeData(dataKey);
        out.writeData(dataNewValue);
        out.writeInt(eventType);
        out.writeInt(partitionId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.sequence = in.readLong();
        this.dataKey = in.readData();
        this.dataNewValue = in.readData();
        this.eventType = in.readInt();
        this.partitionId = in.readInt();
    }

    @Override
    public String toString() {
        return "DefaultSingleEventData{"
                + "creationTime=" + creationTime
                + ", eventType=" + eventType
                + ", sequence=" + sequence
                + ", partitionId=" + partitionId
                + '}';
    }
}
