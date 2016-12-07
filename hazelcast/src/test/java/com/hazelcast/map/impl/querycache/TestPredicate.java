package com.hazelcast.map.impl.querycache;

import com.hazelcast.query.Predicate;

import java.util.Map;

// used by QueryCachePredicateConfigTest$test_whenClassNameIsSet
@SuppressWarnings("unused")
public class TestPredicate implements Predicate {

    @Override
    public boolean apply(Map.Entry mapEntry) {
        return false;
    }
}
