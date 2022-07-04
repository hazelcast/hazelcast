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

package com.hazelcast.client.cp.internal.datastructures.atomicref;

import classloading.domain.Person;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.internal.util.FilteringClassLoader;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertNotNull;

/**
 * Test for issue: https://github.com/hazelcast/hazelcast/issues/17050
 * <p>
 * {@code classloading.domain.Person} class is not on classpath of servers.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AtomicRefIsolatedServersTest extends HazelcastRaftTestSupport {

    @Override
    protected TestHazelcastInstanceFactory createTestFactory() {
        return new TestHazelcastFactory();
    }

    @Test
    public void testCpMode() throws Exception {
        int groupSize = 3;
        Config config = createConfig(groupSize, groupSize);
        startServers(groupSize, config);

        testSetAndGet();
    }

    @Test
    public void testUnsafeMode() throws Exception {
        Config config = new Config();
        startServers(2, config);

        testSetAndGet();
    }

    private void testSetAndGet() throws Exception {
        HazelcastInstance client = ((TestHazelcastFactory) factory).newHazelcastClient();
        IAtomicReference<Object> ref = client.getCPSubsystem().getAtomicReference("test");

        ref.set(new Person());
        Object result = ref.getAsync().toCompletableFuture().get(1, TimeUnit.MINUTES);
        assertNotNull(result);
    }

    private void startServers(int count, Config config) throws Exception {
        spawn(() -> {
            FilteringClassLoader cl = new FilteringClassLoader(singletonList("classloading"), null);
            Thread.currentThread().setContextClassLoader(cl);

            config.setClassLoader(cl);
            factory.newInstances(config, count);
        }).get();
    }
}
