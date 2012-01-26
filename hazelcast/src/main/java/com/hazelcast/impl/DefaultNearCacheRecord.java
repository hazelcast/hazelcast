package com.hazelcast.impl;

import com.hazelcast.nio.Data;

import static com.hazelcast.nio.IOUtil.toObject;

public class DefaultNearCacheRecord implements NearCacheRecord {

    private final Data keyData;
    private Data valueData = null;
    private Object value = null;

    public DefaultNearCacheRecord(Data keyData, Data valueData) {
        super();
        this.keyData = keyData;
        this.valueData = valueData;
    }

    public void setValueData(Data valueData) {
        this.valueData = valueData;
        this.value = null;
    }

    public Data getValueData() {
        return valueData;
    }

    public Data getKeyData() {
        return keyData;
    }

    public boolean hasValueData() {
        return valueData != null;
    }

    public Object getValue() {
        if (value != null) {
            return value;
        }
        if (!hasValueData()) {
            return null;
        }
        value = toObject(valueData);
        return value;
    }

    public void invalidate() {
        setValueData(null);
    }
}
