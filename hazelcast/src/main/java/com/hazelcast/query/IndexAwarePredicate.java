package com.hazelcast.query;

import com.hazelcast.core.MapEntry;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IndexAwarePredicate extends Predicate {
    boolean collectIndexAwarePredicates(List<IndexAwarePredicate> lsIndexAwarePredicates, Map<String, Index<MapEntry>> namedIndexes);

    void collectAppliedIndexes(Set<Index> setAppliedIndexes, Map<String, Index<MapEntry>> namedIndexes);

    Set<MapEntry> filter(Map<String, Index<MapEntry>> namedIndexes);

}
