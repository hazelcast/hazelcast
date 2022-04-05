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

package com.hazelcast.client.partitionservice;

import com.hazelcast.client.impl.spi.ClientPartitionService;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.client.impl.clientside.ClientTestUtil.getHazelcastClientInstanceImpl;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientPartitionServiceLiteMemberTest {

    private TestHazelcastFactory factory;

    @Before
    public void before() {
        factory = new TestHazelcastFactory();
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testWithOnlyLiteMember() {
        factory.newHazelcastInstance(new Config().setLiteMember(true));

        HazelcastInstance client = factory.newHazelcastClient();
        ClientPartitionService clientPartitionService = getClientPartitionService(client);
        assertTrueEventually(() -> assertEquals(271, clientPartitionService.getPartitionCount()));
        assertNull(clientPartitionService.getPartitionOwner(0));

    }

    @Test
    public void testWithLiteMemberAndDataMember() {
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance();
        factory.newHazelcastInstance(new Config().setLiteMember(true));

        TestUtil.warmUpPartitions(hazelcastInstance);
        HazelcastInstance client = factory.newHazelcastClient();
        ClientPartitionService clientPartitionService = getClientPartitionService(client);
        assertTrueEventually(() -> {
            assertNotEquals(0, clientPartitionService.getPartitionCount());
            assertNotNull(clientPartitionService.getPartitionOwner(0));
        });
    }

    private ClientPartitionService getClientPartitionService(HazelcastInstance client) {
        return getHazelcastClientInstanceImpl(client).getClientPartitionService();
    }

}
