package com.hazelcast.map;

import com.hazelcast.query.Predicate;

import java.util.Map;

public class AggregatorPredicate implements Predicate {

    private final Predicate p;

    public AggregatorPredicate(Predicate p){
        this.p = p;
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        return false;
    }
}
