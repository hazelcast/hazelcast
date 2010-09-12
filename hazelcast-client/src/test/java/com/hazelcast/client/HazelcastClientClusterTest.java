/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.TestUtility.newHazelcastClient;
import static org.junit.Assert.assertTrue;

public class HazelcastClientClusterTest {

    @After
    @Before
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testMembershipListener() throws InterruptedException, IOException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastClient client = newHazelcastClient(h1);
        final CountDownLatch memberAddLatch = new CountDownLatch(1);
        final CountDownLatch memberRemoveLatch = new CountDownLatch(1);
        client.getCluster().addMembershipListener(new MembershipListener() {
            public void memberAdded(MembershipEvent membershipEvent) {
                memberAddLatch.countDown();
            }

            public void memberRemoved(MembershipEvent membershipEvent) {
                memberRemoveLatch.countDown();
            }
        });
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        h2.getLifecycleService().shutdown();
        assertTrue(memberAddLatch.await(10, TimeUnit.SECONDS));
        assertTrue(memberRemoveLatch.await(10, TimeUnit.SECONDS));
        client.shutdown();
    }
}
