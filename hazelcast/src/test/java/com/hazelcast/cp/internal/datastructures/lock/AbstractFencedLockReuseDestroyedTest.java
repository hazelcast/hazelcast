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

package com.hazelcast.cp.internal.datastructures.lock;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.lock.FencedLock;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractFencedLockReuseDestroyedTest extends HazelcastRaftTestSupport {

    protected HazelcastInstance[] instances;
    protected String name;
    protected FencedLock lock;

    @Before
    public void setup() {
        instances = createInstances();
        name = getName();
        lock = createFencedLock(name);
        assertNotNull(lock);
    }

    protected abstract HazelcastInstance[] createInstances();

    protected abstract String getName();

    protected abstract FencedLock createFencedLock(String name);

    @Test
    public void testUse_afterDestroy() {
        lock.lock();
        lock.destroy();

        assertFalse(lock.isLocked());

        lock.lock();
        assertTrue(lock.isLocked());
    }

    @Test
    public void testCreate_afterDestroy() {
        lock.lock();
        lock.destroy();

        lock = createFencedLock(name);
        lock.lock();
        assertTrue(lock.isLocked());
    }

    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);

        config.getCPSubsystemConfig().setReuseDestroyedObjectsNames(true);
        return config;
    }

}
