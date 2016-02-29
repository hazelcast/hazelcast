/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.eviction.policies.MapEvictionPolicy;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.LazyEntryViewFromRecord;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.Storage;
import com.hazelcast.map.impl.recordstore.StorageImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.IPartition;
import com.hazelcast.partition.IPartitionService;
import com.hazelcast.util.Clock;

/**
 * Eviction helper methods.
 */
public class EvictorImpl implements Evictor {

    protected final EvictionChecker evictionChecker;
    protected final MapEvictionPolicy evictionPolicy;
    protected final IPartitionService partitionService;

    public EvictorImpl(EvictionChecker evictionChecker, MapEvictionPolicy evictionPolicy, IPartitionService partitionService) {
        this.evictionChecker = evictionChecker;
        this.evictionPolicy = evictionPolicy;
        this.partitionService = partitionService;
    }

    @Override
    public void evict(RecordStore recordStore) {
        Iterable<EntryView> samples = getSamples(recordStore);
        EntryView selectedEntry = evictionPolicy.selectEvictableEntry(samples);

        if (selectedEntry == null) {
            return;
        }

        evictEntry(selectedEntry, recordStore);
    }

    @Override
    public boolean checkEvictable(RecordStore recordStore) {
        return evictionChecker.checkEvictable(recordStore);
    }

    private void evictEntry(EntryView selectedEntry, RecordStore recordStore) {
        Record record = getRecordFromEntryView(selectedEntry);
        Data key = record.getKey();

        if (!recordStore.isLocked(key)) {

            boolean backup = isBackup(recordStore);
            recordStore.evict(key, backup);

            if (!backup) {
                recordStore.doPostEvictionOperations(record, backup);
            }
        }

    }

    // this method is overridden in another context.
    protected Record getRecordFromEntryView(EntryView selectedEntry) {
        return ((LazyEntryViewFromRecord) selectedEntry).getRecord();
    }


    protected boolean isBackup(RecordStore recordStore) {
        int partitionId = recordStore.getPartitionId();
        IPartition partition = partitionService.getPartition(partitionId, false);
        return !partition.isLocal();
    }

    protected Iterable<EntryView> getSamples(RecordStore recordStore) {
        int sampleCount = evictionPolicy.getSampleCount();
        Storage storage = recordStore.getStorage();

        return (Iterable<EntryView>) ((StorageImpl) storage).getRandomSamples(sampleCount);
    }

    protected static long getNow() {
        return Clock.currentTimeMillis();
    }

}
