package com.hazelcast.query;

import com.hazelcast.core.MapEntry;

import java.io.Serializable;

public interface Predicate extends Serializable{
    boolean apply(MapEntry mapEntry);
}