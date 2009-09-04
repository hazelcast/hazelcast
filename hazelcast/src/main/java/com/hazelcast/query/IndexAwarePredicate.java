package com.hazelcast.query;

import com.hazelcast.core.MapEntry;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IndexAwarePredicate extends Predicate {
    boolean collectIndexedPredicates(List<IndexAwarePredicate> lsIndexAwarePredicates);

    boolean filter(Set<MapEntry> results, Map<String, Index<MapEntry>> namedIndexes);
}
