/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.internal.serialization.Data;

import java.io.IOException;
import java.util.Objects;

/**
 * Implementation of {@link com.hazelcast.cache.impl.CacheEventData}.
 *
 * @see com.hazelcast.cache.impl.CacheEventData
 */
@BinaryInterface
@SuppressWarnings({"CyclomaticComplexity", "NPathComplexity"})
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
        out.writeString(name);
        out.writeInt(eventType.getType());
        IOUtil.writeData(out, dataKey);
        IOUtil.writeData(out, dataNewValue);
        IOUtil.writeData(out, dataOldValue);
        out.writeBoolean(isOldValueAvailable);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        name = in.readString();
        eventType = CacheEventType.getByType(in.readInt());
        dataKey = IOUtil.readData(in);
        dataNewValue = IOUtil.readData(in);
        dataOldValue = IOUtil.readData(in);
        isOldValueAvailable = in.readBoolean();
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.CACHE_EVENT_DATA;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public String toString() {
        return "CacheEventDataImpl{"
                + "name='" + name + '\''
                + ", eventType=" + eventType
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CacheEventDataImpl that = (CacheEventDataImpl) o;
        if (isOldValueAvailable != that.isOldValueAvailable) {
            return false;
        }
        if (!Objects.equals(name, that.name)) {
            return false;
        }
        if (eventType != that.eventType) {
            return false;
        }
        if (!Objects.equals(dataKey, that.dataKey)) {
            return false;
        }
        if (!Objects.equals(dataNewValue, that.dataNewValue)) {
            return false;
        }
        return Objects.equals(dataOldValue, that.dataOldValue);
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (eventType != null ? eventType.hashCode() : 0);
        result = 31 * result + (dataKey != null ? dataKey.hashCode() : 0);
        result = 31 * result + (dataNewValue != null ? dataNewValue.hashCode() : 0);
        result = 31 * result + (dataOldValue != null ? dataOldValue.hashCode() : 0);
        result = 31 * result + (isOldValueAvailable ? 1 : 0);
        return result;
    }
}
