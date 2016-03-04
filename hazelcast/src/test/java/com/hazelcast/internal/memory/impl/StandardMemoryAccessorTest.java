package com.hazelcast.internal.memory.impl;

import com.hazelcast.internal.memory.GlobalMemoryAccessor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class StandardMemoryAccessorTest extends BaseMemoryAccessorTest {

    @Override
    protected GlobalMemoryAccessor getMemoryAccessor() {
        return StandardMemoryAccessor.INSTANCE;
    }

    @Test
    @RequiresUnalignedMemoryAccessSupport
    public void test_putGetChar_whenUnaligned() {
        do_test_putGetChar(false);
    }

    @Test
    @RequiresUnalignedMemoryAccessSupport
    public void test_putGetShort_whenUnaligned() {
        do_test_putGetShort(false);
    }

    @Test
    @RequiresUnalignedMemoryAccessSupport
    public void test_putGetInt_whenUnaligned() {
        do_test_putGetInt(false);
    }

    @Test
    @RequiresUnalignedMemoryAccessSupport
    public void test_putGetFloat_whenUnaligned() {
        do_test_putGetFloat(false);
    }

    @Test
    @RequiresUnalignedMemoryAccessSupport
    public void test_putGetLong_whenUnaligned() {
        do_test_putGetLong(false);
    }

    @Test
    @RequiresUnalignedMemoryAccessSupport
    public void test_putGetDouble_whenUnaligned() {
        do_test_putGetDouble(false);
    }
}
