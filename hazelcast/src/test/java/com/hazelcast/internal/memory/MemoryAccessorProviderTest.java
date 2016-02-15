package com.hazelcast.internal.memory;

import com.hazelcast.internal.memory.impl.AlignmentAwareMemoryAccessor;
import com.hazelcast.internal.memory.impl.StandardMemoryAccessor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class MemoryAccessorProviderTest extends UnsafeDependentMemoryAccessorTest {

    @Test
    public void test_getMemoryAccessor_default() {
        assertNotNull(MemoryAccessorProvider.getDefaultMemoryAccessor());
    }

    private void checkStandardMemoryAccessorAvailable() {
        MemoryAccessor memoryAccessor = MemoryAccessorProvider.getMemoryAccessor(MemoryAccessorType.STANDARD);
        if (StandardMemoryAccessor.isAvailable()) {
            assertNotNull(memoryAccessor);
            assertTrue(memoryAccessor instanceof StandardMemoryAccessor);
        }
    }

    private void checkAlignmentAwareMemoryAccessorAvailable() {
        MemoryAccessor memoryAccessor = MemoryAccessorProvider.getMemoryAccessor(MemoryAccessorType.ALIGNMENT_AWARE);
        if (AlignmentAwareMemoryAccessor.isAvailable()) {
            assertNotNull(memoryAccessor);
            assertTrue(memoryAccessor instanceof AlignmentAwareMemoryAccessor);
        }
    }

    @Test
    public void test_getMemoryAccessor_standard() {
        checkStandardMemoryAccessorAvailable();
    }

    @Test
    public void test_getMemoryAccessor_alignmentAware() {
        checkAlignmentAwareMemoryAccessorAvailable();
    }

    @Test
    public void test_getMemoryAccessor_platformAware() {
        if (MemoryAccessorProvider.isUnalignedAccessAllowed()) {
            checkStandardMemoryAccessorAvailable();
        } else {
            checkAlignmentAwareMemoryAccessorAvailable();
        }
    }

}
