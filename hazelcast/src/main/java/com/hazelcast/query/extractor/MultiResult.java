package com.hazelcast.query.extractor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class MultiResult {

    private List<Object> results;

    public MultiResult() {
        this.results = new ArrayList<Object>();
    }

    public void add(Object result) {
        results.add(result);
    }

    public List<Object> getResults() {
        return Collections.unmodifiableList(results);
    }

    public boolean isEmpty() {
        return results.isEmpty();
    }

}
