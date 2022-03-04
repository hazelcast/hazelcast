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

package com.hazelcast.internal.util.collection;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;

import static java.lang.Character.toUpperCase;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class HsaHeapMemoryManagerTest {

    private static final int BLOCK_SIZE = 24;

    private static final Collection<TypeAndSampleValue> unsupportedTypes = asList(
            tsv(boolean.class, false),
            tsv(byte.class, (byte) 0),
            tsv(char.class, (char) 0),
            tsv(int.class, 0),
            tsv(float.class, (float) 0),
            tsv(double.class, (double) 0));


    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    private final HsaHeapMemoryManager memMgr = new HsaHeapMemoryManager();
    private final MemoryAllocator malloc = memMgr.getAllocator();
    private final MemoryAccessor mem = memMgr.getAccessor();

    @Test
    public void when_allocateTwoBlocks_then_eachAddressIsIndepednent() {
        final long addr1 = allocate();
        final long addr2 = allocate();
        for (int offset = 0; offset < BLOCK_SIZE; offset += 8) {
            mem.putLong(addr1 + offset, offset);
            mem.putLong(addr2 + offset, offset + 1);
        }
        for (int offset = 0; offset < BLOCK_SIZE; offset += 8) {
            assertEquals(offset, mem.getLong(addr1 + offset));
            assertEquals(offset + 1, mem.getLong(addr2 + offset));
        }
    }

    @Test
    @RequireAssertEnabled
    public void when_allocateThirdBlock_thenFail() {
        allocate();
        allocate();
        exceptionRule.expect(AssertionError.class);
        allocate();
    }

    @Test
    public void when_allocateFreeAllocate_thenSucceed() {
        final long addr = allocate();
        allocate();
        free(addr);
        allocate();
    }

    @Test
    public void when_allocateFree_then_noLeak() {
        final long addr1 = allocate();
        final long addr2 = allocate();
        free(addr1);
        free(addr2);
        assertEquals(0, memMgr.getUsedMemory());
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void when_allocateUnaligned_then_fail() {
        malloc.allocate(13);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void when_freeWrongSize_then_fail() {
        final long addr1 = allocate();
        malloc.free(addr1, BLOCK_SIZE + 8);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void when_freeWrongAddress_then_fail() {
        free(allocate() + 8);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void when_reallocate_then_failUnsupported() {
        malloc.reallocate(8, BLOCK_SIZE, 2 * BLOCK_SIZE);
    }

    @Test
    public void memReportsLittleEndian() {
        assertFalse(mem.isBigEndian());
    }

    @Test
    public void mostMemoryAccessorOperationsAreUnsupported() throws Exception {
        final Class<? extends MemoryAccessor> c = mem.getClass();
        for (TypeAndSampleValue t : unsupportedTypes) {
            assertUnsupported(c.getMethod(t.getterName(), long.class), null);
            assertUnsupported(c.getMethod(t.putterName(), long.class, t.type), t.sampleValue);
        }
    }

    private void assertUnsupported(Method m, Object value) throws Exception {
        try {
            if (value != null) {
                m.invoke(mem, 8, value);
            } else {
                m.invoke(mem, 8);
            }
            fail();
        } catch (InvocationTargetException e) {
            final Throwable cause = e.getCause();
            assertTrue("Method threw wrong kind of exception: " + cause.getClass().getSimpleName(),
                    cause instanceof UnsupportedOperationException);
        }
    }


    private long allocate() {
        return malloc.allocate(BLOCK_SIZE);
    }

    private void free(long addr) {
        malloc.free(addr, BLOCK_SIZE);
    }

    private static TypeAndSampleValue tsv(Class<?> type, Object sampleValue) {
        return new TypeAndSampleValue(type, sampleValue);
    }

    private static final class TypeAndSampleValue {
        final Class<?> type;
        final Object sampleValue;

        TypeAndSampleValue(Class<?> type, Object sampleValue) {
            this.type = type;
            this.sampleValue = sampleValue;
        }

        String getterName() {
            return "get" + capitalize(type.getSimpleName());
        }

        String putterName() {
            return "put" + capitalize(type.getSimpleName());
        }

        private String capitalize(String n) {
            return toUpperCase(n.charAt(0)) + n.substring(1);
        }
    }
}
