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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ScheduledHazelcastInstanceAwareTest extends ScheduledExecutorServiceTestSupport {

    private HazelcastInstance[] members;
    private IScheduledExecutorService scheduledExecutorService;

    @Before
    public void setup() {
        members = createClusterWithCount(2);
        scheduledExecutorService = getScheduledExecutor(members, randomName());
    }

    @Test
    public void test_hazelcastInstanceIsInjected_whenSchedulingOnSameMember() throws Exception {
        IScheduledFuture<Boolean> injected = scheduledExecutorService.schedule(
                new HazelcastInstanceAwareRunnable(randomNameOwnedBy(members[0])), 200, MILLISECONDS);
        assertTrue("HazelcastInstance should have been injected", injected.get());
    }

    @Test
    public void test_hazelcastInstanceIsInjected_whenSchedulingOnOtherMember() throws Exception {
        IScheduledFuture<Boolean> injected = scheduledExecutorService.schedule(
                new HazelcastInstanceAwareRunnable(randomNameOwnedBy(members[1])), 200, MILLISECONDS);
        assertTrue("HazelcastInstance should have been injected", injected.get());
    }
}
