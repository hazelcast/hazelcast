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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.Collections.synchronizedList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EventRegistrationTest extends HazelcastTestSupport {

    private final ILogger logger = Logger.getLogger(getClass());

    @After
    public void tearDown() throws InterruptedException {
        Hazelcast.shutdownAll();
    }

    @Test
    public void test_eventRegistrations_OnStartup() {
        assertEventRegistrations(3, startInstances(3));
    }

    private HazelcastInstance[] startInstances(int nodeCount) {
        List<HazelcastInstance> instancesList = synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(nodeCount);
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(3);

        for (int i = 0; i < nodeCount; ++i) {
            new Thread(() -> {
                try {
                    Address address = instanceFactory.nextAddress();
                    HazelcastInstance instance = instanceFactory.newHazelcastInstance(address, new Config());
                    instancesList.add(instance);
                } catch (Throwable e) {
                    logger.severe(e);
                } finally {
                    latch.countDown();
                }
            }, "Start thread for node " + i).start();
        }

        assertOpenEventually(latch);

        return instancesList.toArray(new HazelcastInstance[0]);
    }

    private static void assertEventRegistrations(int expected, HazelcastInstance... instances) {
        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                Collection<EventRegistration> regs = getNodeEngineImpl(instance).getEventService().getRegistrations(
                        ProxyServiceImpl.SERVICE_NAME, ProxyServiceImpl.SERVICE_NAME);
                assertEquals(instance + ": " + regs, expected, regs.size());
            }
        });
    }
}
