package com.hazelcast.query.impl;

import java.util.List;

public class IndexValueBatch {
    private Comparable<?> value;
    private List<QueryableEntry> entries;

    public IndexValueBatch(Comparable<?> value, List<QueryableEntry> entries) {
        this.value = value;
        this.entries = entries;
    }

    public Comparable<?> getValue() {
        return value;
    }

    public List<QueryableEntry> getEntries() {
        return entries;
    }
}
