package com.hazelcast.query;

import java.util.List;

public interface IndexAwarePredicate {
    void collectIndexedPredicates(List<IndexedPredicate> lsIndexPredicates);
}
