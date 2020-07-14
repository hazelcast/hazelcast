package com.hazelcast.sql.impl.calcite.opt.physical.index;

public enum IndexCandidateType {
    EQUALS,
    IN,
    GREATER_THAN,
    GREATER_THAN_OR_EQUALS,
    LESS_THAN,
    LESS_THAN_OR_EQUALS;
}
