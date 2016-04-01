package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.serialization.FieldType;

import java.util.List;

interface PortablePosition {
    // used for all field types
    int getStreamPosition();

    int getIndex();

    // used for portables only
    boolean isNull();

    int getLen();

    int getFactoryId();

    int getClassId();

    // determines type of position
    boolean isMultiPosition();

    boolean isEmpty();

    // convenience
    boolean isNullOrEmpty();

    boolean isLast();

    List<PortablePosition> asMultiPosition();

    FieldType getType();
}