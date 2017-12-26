package com.hazelcast.dataset.impl.entryprocessor;

import com.hazelcast.dataset.impl.SegmentRun;

public abstract class EpSegmentRun extends SegmentRun<Object> {

    @Override
    public Object result() {
        // there is no result
        return null;
    }
}
