package com.hazelcast.map.impl.nearcache;

import com.hazelcast.core.ReadOnly;
import com.hazelcast.map.EntryProcessor;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

public class TestReadOnlyProcessor implements EntryProcessor<Integer, Integer, Boolean>, ReadOnly {

    @Override
    public Boolean process(Map.Entry<Integer, Integer> entry) {
        return true;
    }

    @Nullable
    @Override
    public EntryProcessor<Integer, Integer, Boolean> getBackupProcessor() {
        return null;
    }
}
