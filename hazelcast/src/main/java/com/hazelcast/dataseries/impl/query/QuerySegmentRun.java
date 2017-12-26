package com.hazelcast.dataseries.impl.query;

import com.hazelcast.dataseries.impl.SegmentRun;

import java.util.LinkedList;
import java.util.List;

public abstract class QuerySegmentRun extends SegmentRun<List> {
    protected List result = new LinkedList();

    @Override
    public List result() {
        return result;
    }
}
