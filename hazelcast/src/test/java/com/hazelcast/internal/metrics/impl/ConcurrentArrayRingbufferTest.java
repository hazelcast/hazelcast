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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.managementcenter.ConcurrentArrayRingbuffer;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class ConcurrentArrayRingbufferTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private ConcurrentArrayRingbuffer<Integer> rb = new ConcurrentArrayRingbuffer<>(3);

    @Test
    public void test() {
        assertEquals(0, rb.size());
        assertTrue(rb.isEmpty());
        assertEquals(emptyList(), rb.copyFrom(0).elements());
        rb.add(1);
        assertEquals(1, rb.size());
        assertFalse(rb.isEmpty());
        assertEquals(singletonList(1), rb.copyFrom(0).elements());
        rb.add(2);
        assertEquals(2, rb.size());
        assertEquals(asList(1, 2), rb.copyFrom(0).elements());
        rb.add(3);
        assertEquals(3, rb.size());
        assertEquals(asList(1, 2, 3), rb.copyFrom(0).elements());
        rb.add(4);
        assertEquals(3, rb.size());
        for (int i = 5; i < 8; i++) {
            assertEquals(IntStream.range(i - rb.getCapacity(), i).boxed().collect(toList()),
                    rb.copyFrom(0).elements());
            rb.add(i);
            assertEquals(3, rb.size());
        }
    }

    @Test
    public void when_sequenceTooHigh_then_fail() {
        exception.expect(IllegalArgumentException.class);
        rb.get(0);
    }

    @Test
    public void when_sequenceTooLow_then_fail() {
        exception.expect(IllegalArgumentException.class);
        rb.get(-1);
    }

    @Test
    public void test_get() {
        rb.add(1);
        assertEquals(1, (int) rb.get(0));
        rb.add(2);
        assertEquals(1, (int) rb.get(0));
        assertEquals(2, (int) rb.get(1));
        rb.add(3);
        assertEquals(1, (int) rb.get(0));
        assertEquals(2, (int) rb.get(1));
        assertEquals(3, (int) rb.get(2));
        rb.add(4);
        assertEquals(2, (int) rb.get(1));
        assertEquals(3, (int) rb.get(2));
        assertEquals(4, (int) rb.get(3));
    }

    @Test
    public void test_clear() {
        test();
        rb.clear();
        test();
    }

    @Test
    public void test_copyFromSequence() {
        rb.add(1);
        rb.add(2);
        ConcurrentArrayRingbuffer.RingbufferSlice result = rb.copyFrom(0);
        rb.add(3);
        result = rb.copyFrom(result.nextSequence());
        assertEquals(singletonList(3), result.elements());
    }
}
