/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;

/**
 * THis is a test for https://github.com/hazelcast/hazelcast-enterprise/issues/2492
 * The cause of the problem is the new pipeline in 3.11.
 * In the old approach, the channel wasn't registered before the connection was established.
 * So in case of failure, nothing needs to be unregistered.
 * But with the new pipeline the channel gets registered (created) before the connection is established
 * but it didn't get unregistered if the connection could not be established. Leading to a memory leak.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TcpServerConnectionManager_MemoryLeakTest
        extends HazelcastTestSupport {

    @After
    public void after() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void test() {
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        TcpServer networkingService = (TcpServer) getNode(hz1).getServer();

        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();
        hz2.shutdown();

        assertClusterSizeEventually(1, hz1);

        TcpServerConnectionManager connectionManager = networkingService.getConnectionManager(EndpointQualifier.MEMBER);

        assertTrueAllTheTime(() -> assertEquals(0, connectionManager.acceptedChannels.size()), 5);
    }
}
