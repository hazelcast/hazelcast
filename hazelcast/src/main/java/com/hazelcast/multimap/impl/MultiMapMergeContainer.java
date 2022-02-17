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

package com.hazelcast.multimap.impl;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Container for the merge operation of a {@link MultiMap}.
 *
 * @since 3.10
 */
public class MultiMapMergeContainer implements IdentifiedDataSerializable {

    private Data key;
    private Collection<MultiMapRecord> records;
    private long creationTime;
    private long lastAccessTime;
    private long lastUpdateTime;
    private long hits;

    public MultiMapMergeContainer() {
    }

    public MultiMapMergeContainer(Data key, Collection<MultiMapRecord> records, long creationTime, long lastAccessTime,
                                  long lastUpdateTime, long hits) {
        this.key = key;
        this.records = records;
        this.creationTime = creationTime;
        this.lastAccessTime = lastAccessTime;
        this.lastUpdateTime = lastUpdateTime;
        this.hits = hits;
    }

    public Data getKey() {
        return key;
    }

    public Collection<MultiMapRecord> getRecords() {
        return records;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public long getHits() {
        return hits;
    }

    @Override
    public int getFactoryId() {
        return MultiMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.MERGE_CONTAINER;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeData(out, key);
        out.writeInt(records.size());
        for (MultiMapRecord record : records) {
            out.writeObject(record);
        }
        out.writeLong(creationTime);
        out.writeLong(lastAccessTime);
        out.writeLong(lastUpdateTime);
        out.writeLong(hits);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = IOUtil.readData(in);
        int size = in.readInt();
        records = new ArrayList<MultiMapRecord>(size);
        for (int i = 0; i < size; i++) {
            MultiMapRecord record = in.readObject();
            records.add(record);
        }
        creationTime = in.readLong();
        lastAccessTime = in.readLong();
        lastUpdateTime = in.readLong();
        hits = in.readLong();
    }
}
