package com.hazelcast.internal.memory.impl;

import com.hazelcast.internal.memory.GlobalMemoryAccessor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class AlignmentAwareMemoryAccessorTest extends BaseMemoryAccessorTest {

    @Override
    protected GlobalMemoryAccessor getMemoryAccessor() {
        return AlignmentAwareMemoryAccessor.INSTANCE;
    }

    @Test
    public void test_putGetChar_whenUnaligned() {
        do_test_putGetChar(false);
        assert_putGetCharVolatileUnalignedFails();
    }

    @Test
    public void test_putGetShort_whenUnaligned() {
        do_test_putGetShort(false);
        assert_putGetShortVolatileUnalignedFails();
    }

    @Test
    public void test_putGetInt_whenUnaligned() {
        do_test_putGetInt(false);
        assert_putGetIntVolatileUnalignedFails();
    }

    @Test
    public void test_putGetFloat_whenUnaligned() {
        do_test_putGetFloat(false);
        assert_putGetFloatVolatileUnalignedFails();
    }

    @Test
    public void test_putGetLong_whenUnaligned() {
        do_test_putGetLong(false);
        assert_putGetLongVolatileUnalignedFails();
    }

    @Test
    public void test_putGetDouble_whenUnaligned() {
        do_test_putGetDouble(false);
        assert_putGetDoubleVolatileUnalignedFails();
    }

    @Test
    public void test_putGetObject_whenUnaligned() {
        assert_putGetObjectUnalignedFails();
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
