package com.hazelcast.query.impl.getters;

import java.util.ArrayList;
import java.util.List;

public class MultiResultCollector {
    private List<Object> results;

    public MultiResultCollector() {
        this.results = new ArrayList<Object>();
    }

    public void collect(Object result) {
        results.add(result);
    }

    public List<Object> getResults() {
        return results;
    }

    public boolean isEmpty() {
        return results.isEmpty();
    }
}
