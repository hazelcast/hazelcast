/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.memory;

import com.hazelcast.internal.memory.impl.AbstractUnsafeDependentMemoryAccessorTest;
import com.hazelcast.internal.memory.impl.AlignmentAwareMemoryAccessor;
import com.hazelcast.internal.memory.impl.AlignmentUtil;
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
public class GlobalMemoryAccessorRegistryTest extends AbstractUnsafeDependentMemoryAccessorTest {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(GlobalMemoryAccessorRegistry.class);
    }

    @Test
    public void test_getMemoryAccessor_default() {
        assertNotNull(GlobalMemoryAccessorRegistry.getDefaultGlobalMemoryAccessor());
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
        if (AlignmentUtil.isUnalignedAccessAllowed()) {
            checkStandardMemoryAccessorAvailable();
        } else {
            checkAlignmentAwareMemoryAccessorAvailable();
        }
    }

    private void checkStandardMemoryAccessorAvailable() {
        MemoryAccessor memoryAccessor = GlobalMemoryAccessorRegistry.getGlobalMemoryAccessor(GlobalMemoryAccessorType.STANDARD);
        if (StandardMemoryAccessor.isAvailable()) {
            assertNotNull(memoryAccessor);
            assertTrue(memoryAccessor instanceof StandardMemoryAccessor);
        }
    }

    private void checkAlignmentAwareMemoryAccessorAvailable() {
        MemoryAccessor memoryAccessor
                = GlobalMemoryAccessorRegistry.getGlobalMemoryAccessor(GlobalMemoryAccessorType.ALIGNMENT_AWARE);
        if (AlignmentAwareMemoryAccessor.isAvailable()) {
            assertNotNull(memoryAccessor);
            assertTrue(memoryAccessor instanceof AlignmentAwareMemoryAccessor);
        }
    }
}
