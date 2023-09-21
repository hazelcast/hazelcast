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

package com.hazelcast.cp.internal.datastructures.atomiclong;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public abstract class AbstractAtomicLongReuseDestroyedTest extends HazelcastRaftTestSupport {

    protected HazelcastInstance[] instances;
    protected IAtomicLong atomicLong;
    protected String name;

    @Before
    public void setup() {
        instances = createInstances();
        name = getName();
    }

    protected abstract HazelcastInstance[] createInstances();

    protected abstract String getName();

    protected abstract IAtomicLong createAtomicLong(String name);

    @Test
    public void testUse_atomicLong() {
        atomicLong = createAtomicLong(name);
        IAtomicLong atomicLong2 = createAtomicLong(name);
        IAtomicLong atomicLongExc = createAtomicLong(name);

        atomicLong.getAndAdd(5);
        atomicLong2.getAndAdd(5);
        System.out.println("get: " + atomicLong.get());
        assertEquals(10, atomicLong.get());
        System.out.println("get: " + atomicLong2.get());
        assertEquals(10, atomicLong2.get());

        atomicLong.destroy();
        assertThrows(DistributedObjectDestroyedException.class, () -> atomicLongExc.get());

        atomicLong = createAtomicLong(name);
        assertThrows(DistributedObjectDestroyedException.class, () -> atomicLongExc.get());

        atomicLong.getAndAdd(5);
        atomicLong2 = createAtomicLong(name);
        System.out.println("get: " + atomicLong.get());
        assertEquals(5, atomicLong.get());
        System.out.println("get: " + atomicLong2.get());
        assertEquals(5, atomicLong2.get());

    }


//    protected Config createConfig(int cpNodeCount, int groupSize) {
//        Config config = super.createConfig(cpNodeCount, groupSize);
//
//        config.getCPSubsystemConfig().setReuseDestroyedObjectsNames(true);
//        return config;
//    }

}
