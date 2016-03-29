package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.serialization.FieldDefinition;

import java.util.List;

class PortableSinglePosition implements PortablePosition {

    //    enum AccessType {
//        FIELD,
//        ARRAY
//    }
//
    // used for all positions
    FieldDefinition fd;
    //    AccessType accessType;
    int position;

    // poison pills to indicate null-pointer or empty-array
    boolean nil = false;

    // used for arrays only
    int index = -1;
    int len = 0;

    // used for portables only
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
        return nil;
    }

    @Override
    public int getLen() {
        return len;
    }

    @Override
    public boolean isEmpty() {
        return len == 0;
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