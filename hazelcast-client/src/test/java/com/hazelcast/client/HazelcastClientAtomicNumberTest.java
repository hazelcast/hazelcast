/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.config.Config;
import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.client.TestUtility.newHazelcastClient;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class HazelcastClientAtomicNumberTest {

    @Before
    @After
    public void after() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testAtomicLong() {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastClient client = newHazelcastClient(h1);
        AtomicNumber an = client.getAtomicNumber("testAtomicLong");
        assertEquals(0, an.get());
        assertEquals(-1, an.decrementAndGet());
        assertEquals(0, an.incrementAndGet());
        assertEquals(1, an.incrementAndGet());
        assertEquals(2, an.incrementAndGet());
        assertEquals(1, an.decrementAndGet());
        assertEquals(1, an.getAndSet(23));
        assertEquals(28, an.addAndGet(5));
        assertEquals(28, an.get());
        assertEquals(28, an.getAndAdd(-3));
        assertEquals(24, an.decrementAndGet());
        assertFalse(an.compareAndSet(23, 50));
        assertTrue(an.compareAndSet(24, 50));
        assertTrue(an.compareAndSet(50, 0));
    }

    @Test
    public void testSimple() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastClient client = newHazelcastClient(h1);
        final String name = "simple";
        final AtomicNumber nodeAtomicLong = h1.getAtomicNumber(name);
        final AtomicNumber clientAtomicLong = client.getAtomicNumber(name);
        check(nodeAtomicLong, clientAtomicLong, 0L);
        assertEquals(1L, clientAtomicLong.incrementAndGet());
        check(nodeAtomicLong, clientAtomicLong, 1L);
        assertEquals(1L, clientAtomicLong.getAndAdd(1));
        check(nodeAtomicLong, clientAtomicLong, 2L);
        assertEquals(1L, nodeAtomicLong.decrementAndGet());
        check(nodeAtomicLong, clientAtomicLong, 1L);
        assertEquals(2L, clientAtomicLong.addAndGet(1L));
        check(nodeAtomicLong, clientAtomicLong, 2L);
        clientAtomicLong.set(3L);
        check(nodeAtomicLong, clientAtomicLong, 3L);
        assertFalse(nodeAtomicLong.compareAndSet(4L, 1L));
        check(nodeAtomicLong, clientAtomicLong, 3L);
        assertTrue(clientAtomicLong.compareAndSet(3L, 1L));
        check(nodeAtomicLong, clientAtomicLong, 1L);
    }

    private void check(final AtomicNumber nodeAtomicLong,
                       final AtomicNumber clientAtomicLong, final long expectedValue) {
        assertEquals(expectedValue, nodeAtomicLong.get());
        assertEquals(expectedValue, clientAtomicLong.get());
    }
}
