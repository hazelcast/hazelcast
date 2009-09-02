package com.hazelcast.query;

public interface RangedPredicate {
    enum RangeType {
        LESS,
        GREATER,
        LESS_EQUAL,
        GREATER_EQUAL,
        BETWEEN
    }

    RangeType getRangeType();

    Object getFrom();
    
    Object getTo();
}
