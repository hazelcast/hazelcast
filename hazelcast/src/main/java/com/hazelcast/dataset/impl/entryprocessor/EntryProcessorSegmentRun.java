package com.hazelcast.dataset.impl.entryprocessor;

import com.hazelcast.dataset.impl.SegmentRun;

public abstract class EntryProcessorSegmentRun extends SegmentRun<Object> {

    @Override
    public Object result() {
        // there is no result
        return null;
    }
}
