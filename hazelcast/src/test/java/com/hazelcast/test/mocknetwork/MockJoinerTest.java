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

package com.hazelcast.test.mocknetwork;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.Accessors.getAddress;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MockJoinerTest extends HazelcastTestSupport {

    private int nodeCount = 5;

    @Test
    public void serialJoin() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = factory.newHazelcastInstance();
        }

        assertClusterSizeEventually(instances);
    }

    @Test
    public void serialJoin_withNewInstances() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = factory.newInstances(new Config(), nodeCount);

        assertClusterSizeEventually(instances);
    }

    @Test
    public void parallelJoin() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        final Collection<HazelcastInstance> instances = Collections.synchronizedCollection(new ArrayList<HazelcastInstance>());

        final CountDownLatch latch = new CountDownLatch(nodeCount);

        for (int i = 0; i < nodeCount; i++) {
            new Thread() {
                public void run() {
                    HazelcastInstance hz = factory.newHazelcastInstance();
                    instances.add(hz);
                    latch.countDown();
                }
            }.start();
        }

        assertOpenEventually(latch);
        assertEquals(nodeCount, instances.size());

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(nodeCount, instance);
        }
    }

    @Test
    public void restart_master() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance[] instances = factory.newInstances(new Config(), nodeCount);
        assertClusterSizeEventually(instances);

        Address address = getAddress(instances[0]);
        instances[0].getLifecycleService().terminate();

        instances[0] = factory.newHazelcastInstance(address, new Config());
        assertClusterSizeEventually(instances);
    }

    @Test
    public void restart_slave() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance[] instances = factory.newInstances(new Config(), nodeCount);
        assertClusterSizeEventually(instances);

        Address address = getAddress(instances[1]);
        instances[1].getLifecycleService().terminate();

        instances[1] = factory.newHazelcastInstance(address, new Config());
        assertClusterSizeEventually(instances);
    }

    @Test
    public void restart_multipleInParallel() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        final HazelcastInstance[] instances = factory.newInstances(new Config(), nodeCount);
        assertClusterSizeEventually(instances);

        int restartCount = nodeCount / 2;
        final CountDownLatch latch = new CountDownLatch(restartCount);

        for (int i = 0; i < restartCount; i++) {
            final int ix = i;
            new Thread() {
                public void run() {
                    Address address = getAddress(instances[ix]);
                    instances[ix].getLifecycleService().terminate();
                    instances[ix] = factory.newHazelcastInstance(address, new Config());
                    latch.countDown();
                }
            }.start();
        }

        assertOpenEventually(latch);
        assertClusterSizeEventually(instances);
    }

    private void assertClusterSizeEventually(HazelcastInstance... instances) {
        int size = instances.length;
        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(size, instance);
        }
    }
}
