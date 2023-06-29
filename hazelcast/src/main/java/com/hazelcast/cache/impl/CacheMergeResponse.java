package com.hazelcast.cache.impl;

import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes;
import com.hazelcast.wan.impl.CallerProvenance;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Special response class used to provide verbose results for {@link com.hazelcast.cache.ICache}
 * merge operations, specifically from {@link ICacheRecordStore#merge(SplitBrainMergeTypes.CacheMergeTypes, SplitBrainMergePolicy, CallerProvenance)}
 */
public class CacheMergeResponse {
    @Nullable
    private final CacheRecord record;
    @Nonnull
    private final MergeResult result;

    public CacheMergeResponse(@Nullable CacheRecord record, @Nonnull MergeResult result) {
        this.record = record;
        this.result = result;
    }

    @Nullable
    public CacheRecord getRecord() {
        return record;
    }

    @Nonnull
    public MergeResult getResult() {
        return result;
    }

    public enum MergeResult {
        NO_MERGE_APPLIED(false),
        VALUES_ARE_EQUAL(true),
        RECORD_CREATED(true),
        RECORD_UPDATED(true),
        ;

        private final boolean mergeApplied;

        MergeResult (boolean mergeApplied) {
            this.mergeApplied = mergeApplied;
        }

        public boolean isMergeApplied() {
            return mergeApplied;
        }
    }
}
