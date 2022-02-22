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

package com.hazelcast.cp.internal.datastructures.countdownlatch;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.datastructures.countdownlatch.proxy.CountDownLatchProxy;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractCountDownLatchAdvancedTest extends HazelcastRaftTestSupport {

    protected HazelcastInstance[] instances;
    protected ICountDownLatch latch;

    @Before
    public void setup() {
        instances = createInstances();
        latch = createLatch(getName());
        assertNotNull(latch);
    }

    protected abstract HazelcastInstance[] createInstances();

    protected abstract String getName();

    protected abstract ICountDownLatch createLatch(String name);

    @Test
    public void testSuccessfulAwaitClearsWaitTimeouts() {
        latch.trySetCount(1);

        CPGroupId groupId = getGroupId(latch);
        HazelcastInstance leader = leaderInstanceOf(groupId);
        CountDownLatchService service = getNodeEngineImpl(leader).getService(CountDownLatchService.SERVICE_NAME);
        CountDownLatchRegistry registry = service.getRegistryOrNull(groupId);

        CountDownLatch threadLatch = new CountDownLatch(1);
        spawn(() -> {
            try {
                latch.await(10, MINUTES);
                threadLatch.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        assertTrueEventually(() -> {
            assertFalse(registry.getWaitTimeouts().isEmpty());
            assertFalse(registry.getLiveOperations().isEmpty());
        });

        latch.countDown();

        assertOpenEventually(threadLatch);

        assertTrue(registry.getWaitTimeouts().isEmpty());
        assertTrue(registry.getLiveOperations().isEmpty());
    }

    @Test
    public void testFailedAwaitClearsWaitTimeouts() throws InterruptedException {
        latch.trySetCount(1);

        CPGroupId groupId = getGroupId(latch);
        HazelcastInstance leader = leaderInstanceOf(groupId);
        CountDownLatchService service = getNodeEngineImpl(leader).getService(CountDownLatchService.SERVICE_NAME);
        CountDownLatchRegistry registry = service.getRegistryOrNull(groupId);

        boolean success = latch.await(1, TimeUnit.SECONDS);

        assertFalse(success);
        assertTrue(registry.getWaitTimeouts().isEmpty());
        assertTrue(registry.getLiveOperations().isEmpty());
    }

    @Test
    public void testDestroyClearsWaitTimeouts() {
        latch.trySetCount(1);

        CPGroupId groupId = getGroupId(latch);
        HazelcastInstance leader = leaderInstanceOf(groupId);
        CountDownLatchService service = getNodeEngineImpl(leader).getService(CountDownLatchService.SERVICE_NAME);
        CountDownLatchRegistry registry = service.getRegistryOrNull(groupId);

        spawn(() -> {
            try {
                latch.await(10, MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        assertTrueEventually(() -> assertFalse(registry.getWaitTimeouts().isEmpty()));

        latch.destroy();

        assertTrue(registry.getWaitTimeouts().isEmpty());
        assertTrue(registry.getLiveOperations().isEmpty());
    }

    protected CPGroupId getGroupId(ICountDownLatch latch) {
        return ((CountDownLatchProxy) latch).getGroupId();
    }

    protected abstract HazelcastInstance leaderInstanceOf(CPGroupId groupId);
}
