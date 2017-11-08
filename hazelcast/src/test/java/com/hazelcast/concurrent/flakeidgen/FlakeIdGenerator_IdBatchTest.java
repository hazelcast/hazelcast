/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.flakeidgen;

import com.hazelcast.core.FlakeIdGenerator.IdBatch;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FlakeIdGenerator_IdBatchTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void test_iterator() {
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
    public void test_emptyIdBatch() {
        IdBatch b = new IdBatch(1, 10, 0);
        Iterator<Long> it = b.iterator();

        assertFalse(it.hasNext());
        exception.expect(NoSuchElementException.class);
        it.next();
    }
}
