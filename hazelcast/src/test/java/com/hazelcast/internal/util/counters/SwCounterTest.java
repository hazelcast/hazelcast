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

package com.hazelcast.internal.util.counters;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static org.junit.Assert.assertEquals;

/**
 * If Unsafe is available, this will trigger the testing of the {@link SwCounter.UnsafeSwCounter} class. Otherwise
 * the {@link SwCounter.SafeSwCounter} is tested.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SwCounterTest {

    private SwCounter counter;

    @Before
    public void setup() {
        counter = newSwCounter();
    }

    @Test
    public void inc() {
        assertEquals(1, counter.inc());
    }

    @Test
    public void inc_withAmount() {
        assertEquals(10, counter.inc(10));
        assertEquals(10, counter.inc(0));
        assertEquals(0, counter.inc(-10));
    }

    @Test
    public void set() {
        counter.set(100_000);
        assertEquals(100_000, counter.get());
    }

    @Test
    public void test_toString() {
        String s = counter.toString();
        assertEquals("Counter{value=0}", s);
    }
}
