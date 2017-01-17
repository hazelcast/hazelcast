/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduledexecutor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ScheduledHazelcastInstanceAwareTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance[] members;
    protected IScheduledExecutorService scheduledExecutorService;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory(2);
        members = factory.newInstances();
        scheduledExecutorService = members[0].getScheduledExecutorService(randomName());
    }

    @Test
    public void test_hazelcastInstanceIsInjected_whenSchedulingOnSameMember()
            throws ExecutionException, InterruptedException {
        IScheduledFuture<Boolean> injected = scheduledExecutorService.schedule(
                new HazelcastInstanceAwareRunnable(randomNameOwnedBy(members[0])),200, MILLISECONDS);
        assertTrue("HazelcastInstance should have been injected", injected.get());
    }

    @Test
    public void test_hazelcastInstanceIsInjected_whenSchedulingOnOtherMember()
            throws ExecutionException, InterruptedException {
        IScheduledFuture<Boolean> injected = scheduledExecutorService.schedule(
                new HazelcastInstanceAwareRunnable(randomNameOwnedBy(members[1])),200, MILLISECONDS);
        assertTrue("HazelcastInstance should have been injected", injected.get());
    }

    public static class HazelcastInstanceAwareRunnable implements Callable<Boolean>, HazelcastInstanceAware, Serializable,
                                                                  NamedTask {
        private transient volatile HazelcastInstance instance;
        private final String name;

        public HazelcastInstanceAwareRunnable(String name) {
            this.name = name;
        }

        @Override
        public void setHazelcastInstance(final HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Boolean call() {
            return (instance != null);
        }
    }
}
