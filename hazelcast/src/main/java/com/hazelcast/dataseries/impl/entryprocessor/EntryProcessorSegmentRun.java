package com.hazelcast.dataseries.impl.entryprocessor;

import com.hazelcast.dataseries.impl.SegmentRun;

public abstract class EntryProcessorSegmentRun extends SegmentRun<Object> {

    @Override
    public Object result() {
        // there is no result
        return null;
    }
}
