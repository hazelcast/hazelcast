/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static com.hazelcast.client.TestUtility.*;

public class HazelcastClientAtomicNumberTest {
    
    @Before
    @After
    public void after() throws Exception{
        System.out.flush();
        System.err.flush();
        destroyClients();
        Hazelcast.shutdownAll();
    }
    
    @Test
    public void testSimple() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastClient client = newHazelcastClient(h1);
        
        final String name = "simple";
        final AtomicNumber nodeAtomicNumber = h1.getAtomicNumber(name);
        final AtomicNumber clientAtomicNumber = client.getAtomicNumber(name);

        check(nodeAtomicNumber, clientAtomicNumber, 0L);
        
        assertEquals(1L, clientAtomicNumber.incrementAndGet());
        check(nodeAtomicNumber, clientAtomicNumber, 1L);
        
        assertEquals(1L, clientAtomicNumber.getAndAdd(1));
        check(nodeAtomicNumber, clientAtomicNumber, 2L);
        
        assertEquals(1L, clientAtomicNumber.decrementAndGet());
        check(nodeAtomicNumber, clientAtomicNumber, 1L);
        
        assertEquals(2L, clientAtomicNumber.addAndGet(1L));
        check(nodeAtomicNumber, clientAtomicNumber, 2L);
        
        clientAtomicNumber.set(3L);
        check(nodeAtomicNumber, clientAtomicNumber, 3L);
        
        assertFalse(clientAtomicNumber.compareAndSet(4L, 1L));
        check(nodeAtomicNumber, clientAtomicNumber, 3L);
        
        assertTrue(clientAtomicNumber.compareAndSet(3L, 1L));
        check(nodeAtomicNumber, clientAtomicNumber, 1L);
    }

    private void check(final AtomicNumber nodeAtomicNumber,
            final AtomicNumber clientAtomicNumber, final long expectedValue) {
        assertEquals(expectedValue, nodeAtomicNumber.get());
        assertEquals(expectedValue, clientAtomicNumber.get());
    }

}
