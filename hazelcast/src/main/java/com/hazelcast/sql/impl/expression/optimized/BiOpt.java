package com.hazelcast.sql.impl.expression.optimized;

public interface BiOpt<T> {
    T eval(Object obj1, Object obj2);
}
