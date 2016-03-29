package com.hazelcast.internal.serialization.impl;

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

    List<PortablePosition> asMultiPosition();
}