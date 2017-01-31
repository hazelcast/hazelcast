/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Replication operation is the data migration operation of {@link com.hazelcast.cache.impl.CacheRecordStore}.
 * <p>
 * <p>Cache record store's records and configurations will be migrated into their new nodes.
 * <p>
 * Steps;
 * <ul>
 * <li>Serialize all non expired data.</li>
 * <li>Deserialize the data and config.</li>
 * <li>Create the configuration in the new node service.</li>
 * <li>Insert each record into {@link ICacheRecordStore}.</li>
 * </ul>
 * </p>
 * <p><b>Note:</b> This operation is a per partition operation.</p>
 */
public class CacheReplicationOperation extends Operation implements IdentifiedDataSerializable {

    protected final Map<String, Map<Data, CacheRecord>> data = new HashMap<String, Map<Data, CacheRecord>>();
    protected final List<CacheConfig> configs = new ArrayList<CacheConfig>();
    protected final CacheNearCacheStateHolder nearCacheStateHolder = new CacheNearCacheStateHolder(this);

    public CacheReplicationOperation() {
    }

    public CacheReplicationOperation(CachePartitionSegment segment, int replicaIndex) {
        Iterator<ICacheRecordStore> iter = segment.recordStoreIterator();
        while (iter.hasNext()) {
            ICacheRecordStore cacheRecordStore = iter.next();
            CacheConfig cacheConfig = cacheRecordStore.getConfig();
            if (cacheConfig.getAsyncBackupCount() + cacheConfig.getBackupCount() >= replicaIndex) {
                data.put(cacheRecordStore.getName(), cacheRecordStore.getReadOnlyRecords());
            }
        }

        configs.addAll(segment.getCacheConfigs());
        nearCacheStateHolder.prepare(segment);
    }

    @Override
    public void beforeRun() throws Exception {
        // Migrate CacheConfigs first
        ICacheService service = getService();
        for (CacheConfig config : configs) {
            service.putCacheConfigIfAbsent(config);
        }
    }

    @Override
    public void run() throws Exception {
        ICacheService service = getService();
        for (Map.Entry<String, Map<Data, CacheRecord>> entry : data.entrySet()) {
            ICacheRecordStore cache = service.getOrCreateRecordStore(entry.getKey(), getPartitionId());
            cache.clear();
            Map<Data, CacheRecord> map = entry.getValue();

            Iterator<Map.Entry<Data, CacheRecord>> iterator = map.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Data, CacheRecord> next = iterator.next();
                Data key = next.getKey();
                CacheRecord record = next.getValue();
                iterator.remove();
                cache.putRecord(key, record);
            }
        }
        data.clear();

        if (getReplicaIndex() == 0) {
            nearCacheStateHolder.applyState();
        }
    }

    @Override
    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        int confSize = configs.size();
        out.writeInt(confSize);
        for (CacheConfig config : configs) {
            out.writeObject(config);
        }
        int count = data.size();
        out.writeInt(count);
        long now = Clock.currentTimeMillis();
        for (Map.Entry<String, Map<Data, CacheRecord>> entry : data.entrySet()) {
            Map<Data, CacheRecord> cacheMap = entry.getValue();
            int subCount = cacheMap.size();
            out.writeInt(subCount);
            out.writeUTF(entry.getKey());
            for (Map.Entry<Data, CacheRecord> e : cacheMap.entrySet()) {
                final Data key = e.getKey();
                final CacheRecord record = e.getValue();

                if (record.isExpiredAt(now)) {
                    continue;
                }
                out.writeData(key);
                out.writeObject(record);
            }
            // Empty data will terminate the iteration for read in case
            // expired entries were found while serializing, since the
            // real subCount will then be different from the one written
            // before
            out.writeData(null);
        }

        nearCacheStateHolder.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        int confSize = in.readInt();
        for (int i = 0; i < confSize; i++) {
            final CacheConfig config = in.readObject();
            configs.add(config);
        }
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            int subCount = in.readInt();
            String name = in.readUTF();
            Map<Data, CacheRecord> m = new HashMap<Data, CacheRecord>(subCount);
            data.put(name, m);
            // subCount + 1 because of the DefaultData written as the last entry
            // which adds another Data entry at the end of the stream!
            for (int j = 0; j < subCount + 1; j++) {
                Data key = in.readData();
                // Empty data received so reading can be stopped here since
                // since the real object subCount might be different from
                // the number on the stream due to found expired entries
                if (key == null || key.dataSize() == 0) {
                    break;
                }
                CacheRecord record = in.readObject();
                m.put(key, record);
            }
        }

        nearCacheStateHolder.readData(in);
    }

    public boolean isEmpty() {
        return (configs == null || configs.isEmpty()) && (data == null || data.isEmpty());
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.CACHE_REPLICATION;
    }
}
