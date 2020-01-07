/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.LazyEvictableEntryView;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.Storage;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.eviction.EvictionPolicyComparator;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.ThreadUtil.assertRunningOnPartitionThread;

/**
 * Evictor helper methods.
 */
public class EvictorImpl implements Evictor {

    protected final EvictionChecker evictionChecker;
    protected final EvictionPolicyComparator policy;
    protected final IPartitionService partitionService;

    private final int batchSize;

    public EvictorImpl(EvictionPolicyComparator policy,
                       EvictionChecker evictionChecker, int batchSize,
                       IPartitionService partitionService) {

        this.evictionChecker = checkNotNull(evictionChecker);
        this.partitionService = checkNotNull(partitionService);
        this.policy = checkNotNull(policy);
        this.batchSize = batchSize;
    }

    @Override
    public void evict(RecordStore recordStore, Data excludedKey) {
        assertRunningOnPartitionThread();

        for (int i = 0; i < batchSize; i++) {
            EntryView entryView = selectEvictableEntry(recordStore, excludedKey);
            if (entryView == null) {
                return;
            }
            evictEntry(recordStore, entryView);
        }
    }

    @Override
    public void forceEvictByPercentage(RecordStore recordStore, double evictionPercentage) {
        // NOP.
    }

    @SuppressWarnings("checkstyle:rvcheckcomparetoforspecificreturnvalue")
    private EntryView selectEvictableEntry(RecordStore recordStore, Data excludedKey) {
        EntryView excluded = null;
        EntryView selected = null;

        for (EntryView current : getRandomSamples(recordStore)) {
            if (excludedKey != null && excluded == null
                    && getDataKeyFromEntryView(current).equals(excludedKey)) {
                excluded = current;
                continue;
            }

            if (selected == null
                    || policy.compare(current, selected) < 0) {
                selected = current;
            }
        }

        return selected == null ? excluded : selected;
    }

    private void evictEntry(RecordStore recordStore, EntryView selectedEntry) {
        Record record = getRecordFromEntryView(selectedEntry);
        Data dataKey = getDataKeyFromEntryView(selectedEntry);

        if (recordStore.isLocked(dataKey)) {
            return;
        }

        boolean backup = isBackup(recordStore);
        recordStore.evict(dataKey, backup);

        if (!backup) {
            recordStore.doPostEvictionOperations(dataKey, record);
        }
    }

    @Override
    public boolean checkEvictable(RecordStore recordStore) {
        assertRunningOnPartitionThread();

        return evictionChecker.checkEvictable(recordStore);
    }

    // Overridden by EE code
    protected Record getRecordFromEntryView(EntryView evictableEntryView) {
        return ((LazyEvictableEntryView) evictableEntryView).getRecord();
    }

    // Overridden by EE code
    protected Data getDataKeyFromEntryView(EntryView selectedEntry) {
        return ((LazyEvictableEntryView) selectedEntry).getDataKey();
    }

    protected boolean isBackup(RecordStore recordStore) {
        int partitionId = recordStore.getPartitionId();
        IPartition partition = partitionService.getPartition(partitionId, false);
        return !partition.isLocal();
    }

    protected Iterable<EntryView> getRandomSamples(RecordStore recordStore) {
        Storage storage = recordStore.getStorage();
        return storage.getRandomSamples(SAMPLE_COUNT);
    }

    protected static long getNow() {
        return Clock.currentTimeMillis();
    }

    @Override
    public String toString() {
        return "EvictorImpl{"
                + ", evictionPolicyComparator=" + policy
                + ", batchSize=" + batchSize
                + '}';
    }
}
