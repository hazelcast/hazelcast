/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.CacheEntryViews;
import com.hazelcast.cache.impl.event.CacheWanEventPublisher;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;

import java.io.IOException;

import static com.hazelcast.cache.impl.AbstractCacheRecordStore.SOURCE_NOT_AVAILABLE;
import static com.hazelcast.cache.impl.operation.MutableOperation.IGNORE_COMPLETION;

/**
 * Contains a merging entry for split-brain healing with a {@link CacheMergePolicy}.
 */
public class CacheLegacyMergeOperation
        extends AbstractCacheOperation
        implements BackupAwareOperation {

    private CacheMergePolicy mergePolicy;
    private CacheEntryView<Data, Data> mergingEntry;

    public CacheLegacyMergeOperation() {
    }

    public CacheLegacyMergeOperation(String name, Data key, CacheEntryView<Data, Data> entryView, CacheMergePolicy policy) {
        super(name, key);
        mergingEntry = entryView;
        mergePolicy = policy;
    }

    @Override
    public void run() throws Exception {
        backupRecord = cache.merge(mergingEntry, mergePolicy, SOURCE_NOT_AVAILABLE, null, IGNORE_COMPLETION);
    }

    @Override
    public void afterRun() {
        if (backupRecord != null) {
            if (cache.isWanReplicationEnabled()) {
                Data valueData = getNodeEngine().toData(backupRecord.getValue());
                CacheEntryView<Data, Data> entryView = CacheEntryViews.createDefaultEntryView(key, valueData, backupRecord);
                CacheWanEventPublisher publisher = cacheService.getCacheWanEventPublisher();
                publisher.publishWanReplicationUpdate(name, entryView);
            }
        }
    }

    @Override
    public boolean shouldBackup() {
        return backupRecord != null;
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutBackupOperation(name, key, backupRecord);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mergingEntry);
        out.writeObject(mergePolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mergingEntry = in.readObject();
        mergePolicy = in.readObject();
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.LEGACY_MERGE;
    }
}
