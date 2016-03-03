package com.hazelcast.internal.memory.impl;

import com.hazelcast.internal.memory.MemoryAccessStrategy;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class StandardMemoryAccessTest extends BaseMemoryAccessTest {

    @Override
    protected MemoryAccessStrategy<Object> createMemoryAccessStrategy() {
        return new UnsafeBasedMemoryAccessStrategy();
    }

    @Override
    protected MemoryAccessor createMemoryAccessor() {
        return new StandardMemoryAccessor();
    }

}
