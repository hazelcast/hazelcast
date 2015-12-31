/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp.nonblocking.iobalancer;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.internal.ascii.HTTPCommunicator;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import java.io.IOException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class IOBalancerMemoryLeakTest extends HazelcastTestSupport {

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testMemoryLeak() throws IOException {
        Config config = new Config();
        config.getGroupConfig().setName(randomName());
        config.setProperty(GroupProperty.REST_ENABLED, "true");
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        TcpIpConnectionManager connectionManager = (TcpIpConnectionManager) getConnectionManager(instance);
        for (int i = 0; i < 100; i++) {
            communicator.getClusterInfo();
        }
        final IOBalancer ioBalancer = connectionManager.getIoBalancer();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int inHandlerSize = ioBalancer.getInLoadTracker().getHandlers().size();
                int outHandlerSize = ioBalancer.getOutLoadTracker().getHandlers().size();
                Assert.assertEquals(0, inHandlerSize);
                Assert.assertEquals(0, outHandlerSize);
            }
        });
    }

}