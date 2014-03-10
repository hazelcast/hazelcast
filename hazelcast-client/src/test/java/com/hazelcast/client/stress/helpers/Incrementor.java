package com.hazelcast.client.stress.helpers;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import java.util.Map;

public class Incrementor implements EntryProcessor<Object, Integer> {

    public Object process(Map.Entry<Object, Integer> entry) {

        int v = entry.getValue();
        entry.setValue(++v);
        return v;
    }

    @Override
    public EntryBackupProcessor getBackupProcessor() {
        return null;
    }
}