package com.hazelcast.internal.adapter;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;

import java.util.Map;

public class IMapReplaceEntryProcessor implements EntryProcessor<Integer, String> {

    private static final long serialVersionUID = -4826323876651981295L;

    private final String oldString;
    private final String newString;

    public IMapReplaceEntryProcessor(String oldString, String newString) {
        this.oldString = oldString;
        this.newString = newString;
    }

    @Override
    public Object process(Map.Entry<Integer, String> entry) {
        String value = entry.getValue();
        if (value == null) {
            return null;
        }

        String result = value.replace(oldString, newString);
        entry.setValue(result);
        return result;
    }

    @Override
    public EntryBackupProcessor<Integer, String> getBackupProcessor() {
        return null;
    }
}
