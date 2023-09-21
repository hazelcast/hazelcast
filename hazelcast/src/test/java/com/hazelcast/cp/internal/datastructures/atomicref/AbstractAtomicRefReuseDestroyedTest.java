/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.atomicref;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public abstract class AbstractAtomicRefReuseDestroyedTest extends HazelcastRaftTestSupport {

    protected HazelcastInstance[] instances;
    protected IAtomicReference<String> atomicRef;
    protected String name;

    @Before
    public void setup() {
        instances = createInstances();
        name = getName();
    }

    protected abstract HazelcastInstance[] createInstances();

    protected abstract String getName();

    protected abstract IAtomicReference<String> createAtomicRef(String name);

    @Test
    public void testUse_atomicRef() {
        atomicRef = createAtomicRef(name);
        IAtomicReference<String> atomicRef2 = createAtomicRef(name);
        IAtomicReference<String> atomicRefExc = createAtomicRef(name);

        atomicRef.set("a");
        atomicRef2.set("b");
        System.out.println("get: " + atomicRef.get());
        assertEquals("b", atomicRef.get());
        System.out.println("get: " + atomicRef2.get());
        assertEquals("b", atomicRef2.get());

        atomicRef.destroy();
        assertThrows(DistributedObjectDestroyedException.class, () -> atomicRefExc.get());
        //System.out.println("atomicRefExc: " + atomicRefExc.get());

        atomicRef = createAtomicRef(name);
        assertThrows(DistributedObjectDestroyedException.class, () -> atomicRefExc.get());

        atomicRef.set("c");
        atomicRef2 = createAtomicRef(name);
        System.out.println("get: " + atomicRef.get());
        assertEquals("c", atomicRef.get());
        System.out.println("get: " + atomicRef2.get());
        assertEquals("c", atomicRef2.get());

    }


//    protected Config createConfig(int cpNodeCount, int groupSize) {
//        Config config = super.createConfig(cpNodeCount, groupSize);
//
//        config.getCPSubsystemConfig().setReuseDestroyedObjectsNames(true);
//        return config;
//    }

}
