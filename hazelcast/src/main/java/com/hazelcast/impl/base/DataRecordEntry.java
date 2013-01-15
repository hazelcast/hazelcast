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

package com.hazelcast.impl.base;

import com.hazelcast.core.MapEntry;
import com.hazelcast.util.Clock;
import com.hazelcast.impl.Record;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.nio.IOUtil.toObject;

public class DataRecordEntry implements DataSerializable, MapEntry {

    private long cost = 0;
    private long expirationTime = 0;
    private long lastAccessTime = 0;
    private long lastUpdateTime = 0;
    private long creationTime = 0;
    private long version = 0;
    private int hits = 0;
    private boolean valid = true;
    private String name = null;
    private Data keyData = null;
    private Data valueData = null;
    private Long[] indexes;
    private byte[] indexTypes;
    private Object key = null;
    private Object value = null;
    private long lastStoredTime;
    private Address lockAddress = null;
    private int lockThreadId = -1;
    private long remainingIdle = Long.MAX_VALUE;
    Map<Address, Boolean> mapListeners = null;

    public DataRecordEntry() {
    }

    public DataRecordEntry(Record record) {
        this(record, record.getValueData(), false);
    }

    public DataRecordEntry(Record record, boolean includeListeners) {
        this(record, record.getValueData(), includeListeners);
    }

    public DataRecordEntry(Record record, Data value, boolean includeListeners) {
        cost = record.getCost();
        remainingIdle = record.getRemainingIdle();
        expirationTime = record.getExpirationTime();
        lastAccessTime = record.getLastAccessTime();
        lastUpdateTime = record.getLastUpdateTime();
        creationTime = record.getCreationTime();
        lastStoredTime = record.getLastStoredTime();
        version = record.getVersion();
        hits = record.getHits();
        valid = record.isValid();
        name = record.getName();
        keyData = record.getKeyData();
        valueData = value;
        indexes = record.getIndexes();
        indexTypes = record.getIndexTypes();
        DistributedLock lock = record.getLock();
        if (lock != null && lock.getLockCount() > 0) {
            lockAddress = lock.getLockAddress();
            lockThreadId = lock.getLockThreadId();
        }
        if (includeListeners) {
            Map<Address, Boolean> existing = record.getListeners();
            if (existing != null) {
                mapListeners = Collections.unmodifiableMap(existing);
            }
        }
    }

    public void writeData(DataOutput out) throws IOException {
        long now = Clock.currentTimeMillis();
        out.writeBoolean(valid);
        out.writeLong(cost);
        out.writeLong(remainingIdle);
        out.writeLong(expirationTime - now);
        out.writeLong(lastAccessTime - now);
        out.writeLong(lastUpdateTime - now);
        out.writeLong(creationTime - now);
        out.writeLong(lastStoredTime - now);
        out.writeLong(version);
        out.writeInt(hits);
        out.writeUTF(name);
        keyData.writeData(out);
        boolean valueNull = (valueData == null);
        out.writeBoolean(valueNull);
        if (!valueNull) {
            valueData.writeData(out);
        }
        byte indexCount = (indexes == null) ? 0 : (byte) indexes.length;
        out.write(indexCount);
        for (byte i = 0; i < indexCount; i++) {
            out.writeLong(indexes[i]);
            out.write(indexTypes[i]);
        }
        out.writeInt(lockThreadId);
        boolean hasLockAddress = lockAddress != null;
        out.writeBoolean(hasLockAddress);
        if (hasLockAddress) {
            lockAddress.writeData(out);
        }
        int listenerCount = mapListeners == null ? 0 : mapListeners.size();
        out.writeInt(listenerCount);
        if (mapListeners != null) {
            for (Map.Entry<Address, Boolean> listenerEntry : mapListeners.entrySet()) {
                listenerEntry.getKey().writeData(out);
                out.writeBoolean(listenerEntry.getValue());
            }
        }
    }

    public void readData(DataInput in) throws IOException {
        long now = Clock.currentTimeMillis();
        valid = in.readBoolean();
        cost = in.readLong();
        remainingIdle = in.readLong();
        expirationTime = in.readLong() + now;
        lastAccessTime = in.readLong() + now;
        lastUpdateTime = in.readLong() + now;
        creationTime = in.readLong() + now;
        lastStoredTime = in.readLong() + now;
        version = in.readLong();
        hits = in.readInt();
        name = in.readUTF();
        keyData = new Data();
        keyData.readData(in);
        boolean valueNull = in.readBoolean();
        if (!valueNull) {
            valueData = new Data();
            valueData.readData(in);
        }
        byte indexCount = in.readByte();
        if (indexCount > 0) {
            indexes = new Long[indexCount];
            indexTypes = new byte[indexCount];
            for (byte i = 0; i < indexCount; i++) {
                indexes[i] = in.readLong();
                indexTypes[i] = in.readByte();
            }
        }
        lockThreadId = in.readInt();
        boolean hasLockAddress = in.readBoolean();
        if (hasLockAddress) {
            lockAddress = new Address();
            lockAddress.readData(in);
        }
        int listenerCount = in.readInt();
        if (listenerCount > 0) {
            mapListeners = new HashMap<Address, Boolean>(listenerCount);
            for (int i = 0; i < listenerCount; i++) {
                Address addressListener = new Address();
                addressListener.readData(in);
                mapListeners.put(addressListener, in.readBoolean());
            }
        }
    }

    public long getRemainingIdle() {
        return remainingIdle;
    }

    public Object setValue(Object value) {
        throw new UnsupportedOperationException();
    }

    public long getCost() {
        return cost;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public int getHits() {
        return hits;
    }

    public Object getKey() {
        if (key == null) {
            key = toObject(keyData);
        }
        return key;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public long getLastStoredTime() {
        return lastStoredTime;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public String getName() {
        return name;
    }

    public boolean isValid() {
        return valid;
    }

    public boolean hasValue() {
        return valueData != null || value != null;
    }

    public Object getValue() {
        if (value == null) {
            value = toObject(valueData);
        }
        return value;
    }

    public long getVersion() {
        return version;
    }

    public Data getKeyData() {
        return keyData;
    }

    public Data getValueData() {
        return valueData;
    }

    public Long[] getIndexes() {
        return indexes;
    }

    public byte[] getIndexTypes() {
        return indexTypes;
    }

    public Address getLockAddress() {
        return lockAddress;
    }

    public int getLockThreadId() {
        return lockThreadId;
    }

    public Map<Address, Boolean> getListeners() {
        return mapListeners;
    }
}
