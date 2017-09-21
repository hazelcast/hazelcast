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

package com.hazelcast.jet.function;

import com.hazelcast.jet.core.TestUtil.DummyUncheckedTestException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.NoSuchElementException;

import static com.hazelcast.jet.function.DistributedOptional.empty;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class OptionalTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private DistributedOptional<Integer> optional = DistributedOptional.of(123456);

    @Test
    public void testIfPresent() {
        optional.ifPresent(v -> assertEquals(v, optional.get()));
    }

    @Test
    public void testFilterOrElseGet() {
        assertEquals(optional.get(), optional.orElseGet(() -> 10));
        assertEquals(10, empty().orElseGet(() -> 10));
    }

    @Test
    public void testOrElseThrow() {
        DummyUncheckedTestException expectedExc = new DummyUncheckedTestException();
        exception.expect(equalTo(expectedExc));
        empty().orElseThrow(() -> expectedExc);
    }

    @Test
    public void testFilter() {
        // not-null to empty
        assertFalse(optional.filter(v -> v == 0).isPresent());

        // empty to empty
        assertFalse(empty().filter(v -> false).isPresent());
        assertFalse(empty().filter(v -> true).isPresent());

        // not-null to not-null
        assertEquals(optional.get(), optional.filter(v -> true).get());
    }

    @Test
    public void testMap() {
        assertEquals(String.valueOf(optional.get()), optional.map(String::valueOf).get());
    }

    @Test
    public void testHashCode() {
        assertEquals(optional.get().hashCode(), optional.hashCode());
    }

    @Test
    public void testEquals() {
        assertTrue(optional.equals(optional));
        assertFalse(optional.equals("blabla"));
        assertTrue(empty().equals(DistributedOptional.ofNullable(null)));
        assertTrue(DistributedOptional.of(5).equals(DistributedOptional.ofNullable(5)));
    }

    @Test
    public void testFlatMap() {
        // not-null to empty
        assertFalse(optional.flatMap(v -> empty()).isPresent());
        // empty to not-null
        assertFalse(empty().flatMap(v -> optional).isPresent());
        // empty to empty
        assertFalse(empty().flatMap(v -> empty()).isPresent());
        // not-null to not-null
        assertEquals(Integer.valueOf(5), optional.flatMap(v -> DistributedOptional.of(5)).get());
    }

    @Test
    public void when_getOnEmpty_then_noSuchElementException() {
        exception.expect(NoSuchElementException.class);
        empty().get();
    }
}
