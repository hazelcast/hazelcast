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
 */

package com.hazelcast.client;

import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientOutOfMemoryHandlerTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private TestLifecycleListener listener;
    private HazelcastInstance[] instances;
    private ClientOutOfMemoryHandler outOfMemoryHandler;

    @Before
    public void setUp() {
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        listener = new TestLifecycleListener();
        client.getLifecycleService().addLifecycleListener(listener);

        instances = new HazelcastInstance[2];
        instances[0] = ((HazelcastClientProxy) client).client;
        instances[1] = null;

        outOfMemoryHandler = new ClientOutOfMemoryHandler();
    }

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testOnOutOfMemory() {
        try {
            outOfMemoryHandler.onOutOfMemory(new OutOfMemoryError(), instances);
        } catch (Exception ignored) {
            ignore(ignored);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue("The client should be shutdown", listener.isShutdown);
            }
        });
    }

    private static class TestLifecycleListener implements LifecycleListener {

        private volatile boolean isShutdown;

        @Override
        public void stateChanged(LifecycleEvent event) {
            if (LifecycleEvent.LifecycleState.SHUTDOWN.equals(event.getState())) {
                isShutdown = true;
            }
        }
    }
}
