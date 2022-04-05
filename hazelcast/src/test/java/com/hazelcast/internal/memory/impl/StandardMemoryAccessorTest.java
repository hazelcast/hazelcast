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
