/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

/**
 * Implementation of {@link com.hazelcast.cache.impl.CacheEventData}
 *
 * @see com.hazelcast.cache.impl.CacheEventData
 */
public class CacheEventDataImpl
        implements CacheEventData {

    private String name;
    private CacheEventType eventType;
    private Data dataKey;
    private Data dataNewValue;
    private Data dataOldValue;
    private boolean isOldValueAvailable;

    public CacheEventDataImpl() {
    }

    public CacheEventDataImpl(String name, CacheEventType eventType, Data dataKey, Data dataNewValue, Data dataOldValue,
                              boolean isOldValueAvailable) {
        this.name = name;
        this.eventType = eventType;
        this.dataKey = dataKey;
        this.dataNewValue = dataNewValue;
        this.dataOldValue = dataOldValue;
        this.isOldValueAvailable = isOldValueAvailable;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public CacheEventType getCacheEventType() {
        return eventType;
    }

    @Override
    public Data getDataKey() {
        return dataKey;
    }

    @Override
    public Data getDataValue() {
        return dataNewValue;
    }

    @Override
    public Data getDataOldValue() {
        return dataOldValue;
    }

    @Override
    public boolean isOldValueAvailable() {
        return isOldValueAvailable;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(name);
        out.writeInt(eventType.getType());
        out.writeData(dataKey);
        out.writeData(dataNewValue);
        out.writeData(dataOldValue);
        out.writeBoolean(isOldValueAvailable);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        name = in.readUTF();
        eventType = CacheEventType.getByType(in.readInt());
        dataKey = in.readData();
        dataNewValue = in.readData();
        dataOldValue = in.readData();
        isOldValueAvailable = in.readBoolean();
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.CACHE_EVENT_DATA;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }
}
