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

package com.hazelcast.flakeidgen.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FlakeIdGenerator_IdBatchTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testIterator() {
        IdBatch b = new IdBatch(1, 10, 2);
        Iterator<Long> it = b.iterator();

        assertTrue(it.hasNext());
        assertEquals(Long.valueOf(1), it.next());
        assertTrue(it.hasNext());
        assertEquals(Long.valueOf(11), it.next());
        assertFalse(it.hasNext());
        exception.expect(NoSuchElementException.class);
        it.next();
    }

    @Test
    public void testIterator_hasNextMultipleCalls() {
        IdBatch b = new IdBatch(1, 10, 2);
        Iterator<Long> it = b.iterator();

        assertTrue(it.hasNext());
        assertTrue(it.hasNext());
        assertEquals(Long.valueOf(1), it.next());
        assertTrue(it.hasNext());
        assertEquals(Long.valueOf(11), it.next());
        assertFalse(it.hasNext());
        assertFalse(it.hasNext());
        exception.expect(NoSuchElementException.class);
        it.next();
    }

    @Test
    public void testIterator_hasNextNotCalled() {
        IdBatch b = new IdBatch(1, 10, 2);
        Iterator<Long> it = b.iterator();

        assertEquals(Long.valueOf(1), it.next());
        assertEquals(Long.valueOf(11), it.next());
        exception.expect(NoSuchElementException.class);
        it.next();
    }

    @Test
    public void testIterator_emptyIdBatch() {
        IdBatch b = new IdBatch(1, 10, 0);
        Iterator<Long> it = b.iterator();

        assertFalse(it.hasNext());
        exception.expect(NoSuchElementException.class);
        it.next();
    }
}
