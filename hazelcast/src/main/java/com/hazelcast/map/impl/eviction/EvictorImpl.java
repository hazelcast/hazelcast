/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.eviction.MapEvictionPolicy;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.LazyEntryViewFromRecord;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.Storage;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.internal.util.Clock;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.ThreadUtil.assertRunningOnPartitionThread;

/**
 * Evictor helper methods.
 */
public class EvictorImpl implements Evictor {
    protected final EvictionChecker evictionChecker;
    protected final IPartitionService partitionService;
    protected final MapEvictionPolicy mapEvictionPolicy;

    private final int batchSize;

    public EvictorImpl(MapEvictionPolicy mapEvictionPolicy,
                       EvictionChecker evictionChecker, IPartitionService partitionService, int batchSize) {
        this.evictionChecker = checkNotNull(evictionChecker);
        this.partitionService = checkNotNull(partitionService);
        this.mapEvictionPolicy = checkNotNull(mapEvictionPolicy);
        this.batchSize = batchSize;
    }

    @Override
    public void evict(RecordStore recordStore, Data excludedKey) {
        assertRunningOnPartitionThread();

        for (int i = 0; i < batchSize; i++) {
            EntryView evictableEntry = selectEvictableEntry(recordStore, excludedKey);
            if (evictableEntry == null) {
                return;
            }
            evictEntry(recordStore, evictableEntry);
        }
    }

    @Override
    public void forceEvict(RecordStore recordStore) {

    }

    private EntryView selectEvictableEntry(RecordStore recordStore, Data excludedKey) {
        Iterable<EntryView> samples = getSamples(recordStore);
        EntryView excluded = null;
        EntryView selected = null;

        for (EntryView candidate : samples) {
            if (excludedKey != null && excluded == null && getDataKey(candidate).equals(excludedKey)) {
                excluded = candidate;
                continue;
            }

            if (selected == null) {
                selected = candidate;
            } else if (mapEvictionPolicy.compare(candidate, selected) < 0) {
                selected = candidate;
            }
        }

        return selected == null ? excluded : selected;
    }

    private Data getDataKey(EntryView candidate) {
        return getRecordFromEntryView(candidate).getKey();
    }

    private void evictEntry(RecordStore recordStore, EntryView selectedEntry) {
        Record record = getRecordFromEntryView(selectedEntry);
        Data key = record.getKey();

        if (recordStore.isLocked(record.getKey())) {
            return;
        }

        boolean backup = isBackup(recordStore);
        recordStore.evict(key, backup);

        if (!backup) {
            recordStore.doPostEvictionOperations(record);
        }
    }

    @Override
    public boolean checkEvictable(RecordStore recordStore) {
        assertRunningOnPartitionThread();

        return evictionChecker.checkEvictable(recordStore);
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
        Storage storage = recordStore.getStorage();
        return storage.getRandomSamples(SAMPLE_COUNT);
    }

    protected static long getNow() {
        return Clock.currentTimeMillis();
    }

    @Override
    public String toString() {
        return "EvictorImpl{"
                + ", mapEvictionPolicy=" + mapEvictionPolicy
                + ", batchSize=" + batchSize
                + '}';
    }
}
