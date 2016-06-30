package com.hazelcast.map;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Custom merge policy implementation that causes deletion of related entry.
 */
class TestDeleteMergePolicy implements MapMergePolicy {

    @Override
    public Object merge(String mapName, EntryView mergingEntry, EntryView existingEntry) {
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }
}
