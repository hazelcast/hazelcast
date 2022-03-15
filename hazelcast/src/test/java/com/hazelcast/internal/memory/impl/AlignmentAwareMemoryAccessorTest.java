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
    @Override
    public void test_compareAndSwapInt_whenUnaligned() {
        super.test_compareAndSwapInt_whenUnaligned();
    }

    @Test(expected = IllegalArgumentException.class)
    @Override
    public void test_compareAndSwapLong_whenUnaligned() {
        super.test_compareAndSwapLong_whenUnaligned();
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_compareAndSwapObject_whenUnaligned() {
        do_test_compareAndSwapObject(false);
    }

    @Test(expected = IllegalArgumentException.class)
    @Override
    public void test_putOrderedInt_whenUnaligned() {
        super.test_putOrderedInt_whenUnaligned();
    }

    @Test(expected = IllegalArgumentException.class)
    @Override
    public void test_putOrderedLong_whenUnaligned() {
        super.test_putOrderedLong_whenUnaligned();
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_putOrderedObject_whenUnaligned() {
        do_test_putOrderedObject(false);
    }

}
