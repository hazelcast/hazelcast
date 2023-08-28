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

package com.hazelcast.cp.internal.datastructures.countdownlatch;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public abstract class AbstractCountDownLatchDestroyedTest extends HazelcastRaftTestSupport {

    protected HazelcastInstance[] instances;
    protected String name;
    protected ICountDownLatch latch;

    @Before
    public void setup() {
        instances = createInstances();
        name = getName();
        latch = createLatch(getName());
        assertNotNull(latch);
    }

    protected abstract HazelcastInstance[] createInstances();

    protected abstract String getName();

    protected abstract ICountDownLatch createLatch(String name);

    @Test
    public void testUse_afterDestroy() {
        latch.trySetCount(1);
        latch.destroy();

        assertEquals(0, latch.getCount());
        latch.trySetCount(1);
        assertEquals(1, latch.getCount());
    }

    @Test
    public void testCreate_afterDestroy() {
        latch.trySetCount(1);
        latch.destroy();

        latch = createLatch(name);
        assertEquals(0, latch.getCount());
    }

    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);

        config.getCPSubsystemConfig().setReuseDestroyedObjectsNames(true);
        return config;
    }

}
