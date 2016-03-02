package com.hazelcast.internal.memory.impl;

import com.hazelcast.internal.memory.GlobalMemoryAccessor;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class AlignmentAwareMemoryAccessTest extends BaseMemoryAccessTest {

    @Override
    protected GlobalMemoryAccessor memoryAccessor() {
        return AlignmentAwareMemoryAccessor.INSTANCE;
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_putGetObject_whenUnaligned() {
        do_test_putGetObject(false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_compareAndSwapInt_whenUnaligned() {
        super.test_compareAndSwapInt_whenUnaligned();
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_compareAndSwapLong_whenUnaligned() {
        super.test_compareAndSwapLong_whenUnaligned();
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_compareAndSwapObject_whenUnaligned() {
        do_test_compareAndSwapObject(false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_putOrderedInt_whenUnaligned() {
        super.test_putOrderedInt_whenUnaligned();
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_putOrderedLong_whenUnaligned() {
        super.test_putOrderedLong_whenUnaligned();
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_putOrderedObject_whenUnaligned() {
        do_test_putOrderedObject(false);
    }

}
