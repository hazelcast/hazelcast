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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public abstract class AbstractAtomicRefReuseDestroyedTest extends HazelcastRaftTestSupport {

    protected HazelcastInstance[] instances;
    protected IAtomicReference<String> atomicRef;
    protected String name;

    @Before
    public void setup() {
        instances = createInstances();
        name = getName();
        atomicRef = createAtomicRef(name);
        assertNotNull(atomicRef);
    }

    protected abstract HazelcastInstance[] createInstances();

    protected abstract String getName();

    protected abstract IAtomicReference<String> createAtomicRef(String name);

    @Test
    public void testUse_afterDestroy() {
        atomicRef.set("str1");
        atomicRef.destroy();
        assertNull(atomicRef.get());
    }

    @Test
    public void testCreate_afterDestroy() {
        atomicRef.set("str1");
        atomicRef.destroy();

        atomicRef = createAtomicRef(name);
        assertNull(atomicRef.get());
    }

    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);

        config.getCPSubsystemConfig().setReuseDestroyedObjectsNames(true);
        return config;
    }

}
