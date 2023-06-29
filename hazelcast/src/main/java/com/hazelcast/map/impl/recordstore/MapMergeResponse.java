package com.hazelcast.map.impl.recordstore;

import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes;
import com.hazelcast.wan.impl.CallerProvenance;

/**
 * Special response class used to provide verbose results for {@link com.hazelcast.map.IMap}
 * merge operations, specifically from {@link RecordStore#merge(SplitBrainMergeTypes.MapMergeTypes, SplitBrainMergePolicy, CallerProvenance)}
 */
public enum MapMergeResponse {
    NO_MERGE_APPLIED(false),
    VALUES_ARE_EQUAL(true),
    RECORD_REMOVED(true),
    RECORD_CREATED(true),
    RECORD_UPDATED(true),
    ;

    private final boolean mergeApplied;

    MapMergeResponse (boolean mergeApplied) {
        this.mergeApplied = mergeApplied;
    }

    public boolean isMergeApplied() {
        return mergeApplied;
    }
}
