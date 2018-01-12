package com.hazelcast.dataset.impl.projection;

import com.hazelcast.dataset.impl.SegmentRun;
import com.hazelcast.util.function.Consumer;

public abstract class ProjectionSegmentRun extends SegmentRun<Object> {

    public Consumer consumer;

    //todo: we need to return a result in case of a projection
    @Override
    public Object result() {
        return null;
    }
}
