package com.hazelcast.query;

import java.util.List;

public interface IndexAwarePredicate extends Predicate{
    boolean collectIndexedPredicates(List<IndexedPredicate> lsIndexPredicates);
}
