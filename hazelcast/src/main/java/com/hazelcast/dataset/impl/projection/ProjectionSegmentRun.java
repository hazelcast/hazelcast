package com.hazelcast.dataset.impl.projection;

import com.hazelcast.dataset.impl.SegmentRun;
import com.hazelcast.util.function.Consumer;

public abstract class ProjectionSegmentRun extends SegmentRun<Object> {

    public Consumer consumer;

    @Override
    public Object result() {
        return null;
    }
}
