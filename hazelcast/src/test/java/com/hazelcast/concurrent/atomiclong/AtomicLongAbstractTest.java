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

package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IFunction;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.util.EmptyStatement;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AtomicLongAbstractTest extends HazelcastTestSupport {

    protected HazelcastInstance[] instances;
    protected IAtomicLong atomicLong;

    @Before
    public void setup() {
        instances = newInstances();
        HazelcastInstance local = instances[0];
        HazelcastInstance target = instances[instances.length - 1];
        String name = generateKeyOwnedBy(target);
        atomicLong = local.getAtomicLong(name);
    }

    protected abstract HazelcastInstance[] newInstances();

    @Test
    public void testSet() {
        atomicLong.set(271);
        assertEquals(271, atomicLong.get());
    }

    @Test
    public void testGet() {
        assertEquals(0, atomicLong.get());
    }

    @Test
    public void testDecrementAndGet() {
        assertEquals(-1, atomicLong.decrementAndGet());
        assertEquals(-2, atomicLong.decrementAndGet());
    }

    @Test
    public void testIncrementAndGet() {
        assertEquals(1, atomicLong.incrementAndGet());
        assertEquals(2, atomicLong.incrementAndGet());
    }

    @Test
    public void testGetAndSet() {
        assertEquals(0, atomicLong.getAndSet(271));
        assertEquals(271, atomicLong.get());
    }

    @Test
    public void testAddAndGet() {
        assertEquals(271, atomicLong.addAndGet(271));
    }

    @Test
    public void testGetAndAdd() {
        assertEquals(0, atomicLong.getAndAdd(271));
        assertEquals(271, atomicLong.get());
    }

    @Test
    public void testCompareAndSet_whenSuccess() {
        assertTrue(atomicLong.compareAndSet(0, 271));
        assertEquals(271, atomicLong.get());
    }

    @Test
    public void testCompareAndSet_whenNotSuccess() {
        assertFalse(atomicLong.compareAndSet(172, 0));
        assertEquals(0, atomicLong.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void apply_whenCalledWithNullFunction() {
        atomicLong.apply(null);
    }

    @Test
    public void apply() {
        assertEquals(new Long(1), atomicLong.apply(new AddOneFunction()));
        assertEquals(0, atomicLong.get());
    }

    @Test
    public void apply_whenException() {
        atomicLong.set(1);
        try {
            atomicLong.apply(new FailingFunction());
            fail();
        } catch (ExpectedRuntimeException expected) {
            EmptyStatement.ignore(expected);
        }

        assertEquals(1, atomicLong.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void alter_whenCalledWithNullFunction() {
        atomicLong.alter(null);
    }

    @Test
    public void alter_whenException() {
        atomicLong.set(10);

        try {
            atomicLong.alter(new FailingFunction());
            fail();
        } catch (ExpectedRuntimeException expected) {
            EmptyStatement.ignore(expected);
        }

        assertEquals(10, atomicLong.get());
    }

    @Test
    public void alter() {
        atomicLong.set(10);
        atomicLong.alter(new AddOneFunction());
        assertEquals(11, atomicLong.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void alterAndGet_whenCalledWithNullFunction() {
        atomicLong.alterAndGet(null);
    }

    @Test
    public void alterAndGet_whenException() {
        atomicLong.set(10);

        try {
            atomicLong.alterAndGet(new FailingFunction());
            fail();
        } catch (ExpectedRuntimeException expected) {
            EmptyStatement.ignore(expected);
        }

        assertEquals(10, atomicLong.get());
    }

    @Test
    public void alterAndGet() {
        atomicLong.set(10);
        assertEquals(11, atomicLong.alterAndGet(new AddOneFunction()));
        assertEquals(11, atomicLong.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void getAndAlter_whenCalledWithNullFunction() {
        atomicLong.getAndAlter(null);
    }

    @Test
    public void getAndAlter_whenException() {
        atomicLong.set(10);

        try {
            atomicLong.getAndAlter(new FailingFunction());
            fail();
        } catch (ExpectedRuntimeException expected) {
            EmptyStatement.ignore(expected);
        }

        assertEquals(10, atomicLong.get());
    }

    @Test
    public void getAndAlter() {
        atomicLong.set(10);
        assertEquals(10, atomicLong.getAndAlter(new AddOneFunction()));
        assertEquals(11, atomicLong.get());
    }

    @Test
    public void testDestroy() {
        atomicLong.set(23);
        atomicLong.destroy();

        assertEquals(0, atomicLong.get());
    }

    private static class AddOneFunction implements IFunction<Long, Long> {
        @Override
        public Long apply(Long input) {
            return input + 1;
        }
    }

    protected static class FailingFunction implements IFunction<Long, Long> {
        @Override
        public Long apply(Long input) {
            throw new ExpectedRuntimeException();
        }
    }
}
