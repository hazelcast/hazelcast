package com.hazelcast.internal.memory.impl;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class StandardMemoryAccessorTest extends BaseMemoryAccessorTest {

    @Override
    protected MemoryAccessor createMemoryAccessor() {
        return new StandardMemoryAccessor();
    }

}
