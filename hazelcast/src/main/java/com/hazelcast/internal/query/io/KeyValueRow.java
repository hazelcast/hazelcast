package com.hazelcast.internal.query.io;

import com.hazelcast.internal.query.exec.MapScanExec;

public class KeyValueRow implements Row {

    private final MapScanExec exec;

    private Object key;
    private Object val;

    public KeyValueRow(MapScanExec exec) {
        this.exec = exec;
    }

    public Object getKey() {
        return key;
    }

    public Object getVal() {
        return val;
    }

    public void setKeyValue(Object key, Object val) {
        this.key = key;
        this.val = val;
    }

    @Override
    public Object get(int idx) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int columnCount() {
        throw new UnsupportedOperationException();
    }

    public Object get(String path) {
        return exec.extract(key, val, path);
    }
}
