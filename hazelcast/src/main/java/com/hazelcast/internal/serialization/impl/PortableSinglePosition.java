package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.serialization.FieldDefinition;

import java.util.List;

class PortableSinglePosition implements PortablePosition {
    // used for all field types
    FieldDefinition fd;
    int position;
    int index = -1;

    // used for portables only
    boolean isNull = false;
    int len = -1;
    int factoryId;
    int classId;

    @Override
    public int getStreamPosition() {
        return position;
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public boolean isNull() {
        return isNull;
    }

    @Override
    public int getLen() {
        return len;
    }

    @Override
    public int getFactoryId() {
        return factoryId;
    }

    @Override
    public int getClassId() {
        return classId;
    }

    @Override
    public boolean isMultiPosition() {
        return false;
    }

    @Override
    public List<PortablePosition> asMultiPosition() {
        throw new RuntimeException("Not a multi-position!");
    }
}