package com.hazelcast.internal.serialization.impl;


import com.hazelcast.nio.serialization.FieldType;

import java.util.List;

class PortableMultiPosition extends PortableSinglePosition {

    private final List<PortablePosition> positions;

    public PortableMultiPosition(List<PortablePosition> positions) {
        this.positions = positions;
    }

    @Override
    public boolean isMultiPosition() {
        return true;
    }

    @Override
    public FieldType getType() {
        if (positions.isEmpty()) {
            return null;
        } else {
            return positions.iterator().next().getType();
        }
    }

    @Override
    public List<PortablePosition> asMultiPosition() {
        return positions;
    }

}
