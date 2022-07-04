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

package com.hazelcast.internal.longregister;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public abstract class LongRegisterAbstractTest extends HazelcastTestSupport {

    protected HazelcastInstance[] instances;
    protected IAtomicLong longRegister;

    @Before
    public void setup() {
        instances = newInstances();
        HazelcastInstance local = instances[0];
        HazelcastInstance target = instances[instances.length - 1];
        String name = generateKeyOwnedBy(target);
        longRegister = local.getDistributedObject(LongRegisterService.SERVICE_NAME, name);
    }

    protected abstract HazelcastInstance[] newInstances();

    @Test
    public void testSet() {
        longRegister.set(271);
        assertEquals(271, longRegister.get());
    }

    @Test
    public void testGet() {
        assertEquals(0, longRegister.get());
    }

    @Test
    public void testDecrementAndGet() {
        assertEquals(-1, longRegister.decrementAndGet());
        assertEquals(-2, longRegister.decrementAndGet());
    }

    @Test
    public void testGetAndDecrement() {
        assertEquals(0, longRegister.getAndDecrement());
        assertEquals(-1, longRegister.getAndDecrement());
    }

    @Test
    public void testIncrementAndGet() {
        assertEquals(1, longRegister.incrementAndGet());
        assertEquals(2, longRegister.incrementAndGet());
    }

    @Test
    public void testGetAndIncrement() {
        assertEquals(0, longRegister.getAndIncrement());
        assertEquals(1, longRegister.getAndIncrement());
    }

    @Test
    public void testGetAndSet() {
        assertEquals(0, longRegister.getAndSet(271));
        assertEquals(271, longRegister.get());
    }

    @Test
    public void testAddAndGet() {
        assertEquals(271, longRegister.addAndGet(271));
    }

    @Test
    public void testGetAndAdd() {
        assertEquals(0, longRegister.getAndAdd(271));
        assertEquals(271, longRegister.get());
    }

    @Test
    public void testDestroy() {
        longRegister.set(23);
        longRegister.destroy();

        assertEquals(0, longRegister.get());
    }

}
