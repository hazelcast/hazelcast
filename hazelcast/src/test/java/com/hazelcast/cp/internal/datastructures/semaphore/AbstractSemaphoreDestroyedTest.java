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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public abstract class AbstractSemaphoreDestroyedTest extends HazelcastRaftTestSupport {

    protected HazelcastInstance[] instances;
    protected String name;
    protected ISemaphore semaphore;

    @Before
    public void setup() {
        instances = createInstances();
        name = getName();
        semaphore = createSemaphore(name);
        assertNotNull(semaphore);
    }

    protected abstract HazelcastInstance[] createInstances();

    protected abstract String getName();

    protected abstract ISemaphore createSemaphore(String name);

    @Test
    public void testUse_afterDestroy() {
        semaphore.init(1);
        semaphore.destroy();

        assertEquals(0, semaphore.availablePermits());
        semaphore.init(1);
        assertEquals(1, semaphore.availablePermits());
    }

    @Test
    public void testCreate_afterDestroy() {
        semaphore.init(1);
        semaphore.destroy();

        semaphore = createSemaphore(name);
        assertEquals(0, semaphore.availablePermits());
    }

    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);

        config.getCPSubsystemConfig().setReuseDestroyedObjectsNames(true);
        return config;
    }

}
