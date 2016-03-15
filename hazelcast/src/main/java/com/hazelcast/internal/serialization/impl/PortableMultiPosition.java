package com.hazelcast.internal.serialization.impl;


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
    public List<PortablePosition> asMultiPosition() {
        return positions;
    }

}
