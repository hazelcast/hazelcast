package com.hazelcast.internal.memory.impl;

import com.hazelcast.internal.memory.GlobalMemoryAccessor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class StandardMemoryAccessTest extends BaseMemoryAccessTest {

    @Override
    protected GlobalMemoryAccessor memoryAccessor() {
        return StandardMemoryAccessor.INSTANCE;
    }

}
