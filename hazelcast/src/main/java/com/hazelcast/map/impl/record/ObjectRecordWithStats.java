package com.hazelcast.map.impl.record;

import com.hazelcast.nio.serialization.Data;

class ObjectRecordWithStats extends AbstractRecordWithStats<Object> {

    private Object value;

    public ObjectRecordWithStats() {
    }

    public ObjectRecordWithStats(Data key, Object value) {
        super(key);
        this.value = value;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public void invalidate() {
        value = null;
    }

    @Override
    public long getCost() {
        return 0L;
    }
}
