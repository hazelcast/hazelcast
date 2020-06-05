/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.FilteringClassLoader;
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
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AtomicRefIsolatedServersTest extends HazelcastRaftTestSupport {

    @Override
    protected TestHazelcastInstanceFactory createTestFactory() {
        return new TestHazelcastFactory();
    }

    @Test
    public void test() throws Exception {
        final int groupSize = 3;
        Config config = createConfig(groupSize, groupSize);
        startServers(groupSize, config);

        HazelcastInstance client = ((TestHazelcastFactory) factory).newHazelcastClient();
        IAtomicReference<Object> ref = client.getCPSubsystem().getAtomicReference("test");

        ref.set(new Person());
        Object result = ref.getAsync().get(1, TimeUnit.MINUTES);
        assertNotNull(result);
    }

    private void startServers(final int count, final Config config) {
        spawn(new Runnable() {
            @Override
            public void run() {
                FilteringClassLoader cl = new FilteringClassLoader(singletonList("classloading"), null);
                Thread.currentThread().setContextClassLoader(cl);

                config.setClassLoader(cl);
                factory.newInstances(config, count);
            }
        });
    }
}
