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

package com.hazelcast.cache.operation;

import com.hazelcast.cache.CachePartitionSegment;
import com.hazelcast.cache.CacheRecordStore;
import com.hazelcast.cache.CacheService;
import com.hazelcast.cache.ICacheRecordStore;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.map.NearCache;
import com.hazelcast.map.record.Record;
import com.hazelcast.map.record.RecordReplicationInfo;
import com.hazelcast.map.record.RecordStatistics;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author mdogan 05/02/14
 */
public final class CacheReplicationOperation extends AbstractOperation {

    final Map<String, Map<Data,Record>> source;

    final Map<String, Map<Data, RecordHolder>> destination;

    public CacheReplicationOperation() {
        source = null;
        destination = new HashMap<String, Map<Data, RecordHolder>>();
    }

    public CacheReplicationOperation(CachePartitionSegment segment, int replicaIndex) {
        source = new HashMap<String, Map<Data, Record>>();
        destination = null;

        Iterator<ICacheRecordStore> iter = segment.cacheIterator();
        while (iter.hasNext()) {
            ICacheRecordStore next = iter.next();
            CacheConfig cacheConfig = next.getConfig();
            if (cacheConfig.getAsyncBackupCount() + cacheConfig.getBackupCount() >= replicaIndex) {
                source.put(next.getName(), next.getReadOnlyRecords());
            }
        }
    }

    @Override
    public final void beforeRun() throws Exception {

    }

    @Override
    public void run() throws Exception {
        CacheService service = getService();
        for (Map.Entry<String, Map<Data, RecordHolder>> entry : destination.entrySet()) {
            ICacheRecordStore cache = service.getOrCreateCache(entry.getKey(), getPartitionId());
            Map<Data, RecordHolder> map = entry.getValue();

            Iterator<Map.Entry<Data, RecordHolder>> iter = map.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Data, RecordHolder> next = iter.next();
                Data key = next.getKey();
                RecordHolder holder = next.getValue();
                iter.remove();
                cache.own(key, holder.value, holder.statistics);
            }
        }
        destination.clear();
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        int count = source.size();
        out.writeInt(count);
        if (count > 0) {
            long now = Clock.currentTimeMillis();
            for (Map.Entry<String, Map<Data, Record>> entry : source.entrySet()) {
                Map<Data, Record> cacheMap = entry.getValue();
                int subCount = cacheMap.size();
                out.writeInt(subCount);
                if (subCount > 0) {
                    out.writeUTF(entry.getKey());
                    for (Map.Entry<Data, Record> e : cacheMap.entrySet()) {
                        Record record = e.getValue();

                        Data key = record.getKey();
                        Object value = record.getValue();
                        RecordStatistics statistics = record.getStatistics();

                        long expirationTime = statistics.getExpirationTime();
                        if (expirationTime > now) {
                            out.writeObject(key);
                            out.writeObject(value);
                            out.writeObject(statistics);
                        }

                        subCount--;
                    }
                    if (subCount != 0) {
                        throw new AssertionError("Cache iteration error, count is not zero!" + subCount);
                    }
                }

            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int count = in.readInt();
        if (count > 0) {
            for (int i = 0; i < count; i++) {
                int subCount = in.readInt();
                if (subCount > 0) {
                    String name = in.readUTF();
                    Map<Data, RecordHolder> m = new HashMap<Data, RecordHolder>(subCount);
                    destination.put(name, m);
                    for (int j = 0; j < subCount; j++) {
                        Data key = in.readObject();
                        Object value = in.readObject();
                        RecordStatistics recordStatistics = in.readObject();
                        m.put(key, new RecordHolder(value, recordStatistics));
                    }
                }
            }
        }
    }

    public boolean isEmpty() {
        return source == null || source.isEmpty();
    }

    private class RecordHolder {
        final Object value;
        final RecordStatistics statistics;

        private RecordHolder(Object value, RecordStatistics statistics) {
            this.value = value;
            this.statistics = statistics;
        }
    }
}
