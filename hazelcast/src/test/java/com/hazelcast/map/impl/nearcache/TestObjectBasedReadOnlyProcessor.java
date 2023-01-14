package com.hazelcast.map.impl.nearcache;

import com.hazelcast.core.ReadOnly;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.query.SampleTestObjects;

import javax.annotation.Nullable;
import java.util.Map;

public class TestObjectBasedReadOnlyProcessor implements EntryProcessor<Integer, SampleTestObjects.Employee, Boolean>, ReadOnly {

    @Override
    public Boolean process(Map.Entry<Integer, SampleTestObjects.Employee> entry) {
        return true;
    }

    @Override
    @Nullable
    public EntryProcessor<Integer, SampleTestObjects.Employee, Boolean> getBackupProcessor() {
        return null;
    }
}
