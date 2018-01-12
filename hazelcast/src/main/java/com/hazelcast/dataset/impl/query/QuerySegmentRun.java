package com.hazelcast.dataset.impl.query;

import com.hazelcast.dataset.impl.SegmentRun;

import java.util.LinkedList;
import java.util.List;

public abstract class QuerySegmentRun extends SegmentRun<List> {
    protected List result = new LinkedList();

    @Override
    public List result() {
        return result;
    }
}
