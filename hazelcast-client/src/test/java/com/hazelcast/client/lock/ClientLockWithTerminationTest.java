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

package com.hazelcast.client.lock;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientLockWithTerminationTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance node2;
    private HazelcastInstance client1;
    private HazelcastInstance client2;

    private String keyOwnedByNode2;

    @Before
    public void setup() throws InterruptedException {
        hazelcastFactory.newHazelcastInstance();
        node2 = hazelcastFactory.newHazelcastInstance();
        client1 = hazelcastFactory.newHazelcastClient();
        client2 = hazelcastFactory.newHazelcastClient();

        HazelcastTestSupport.warmUpPartitions(node2);
        keyOwnedByNode2 = HazelcastTestSupport.generateKeyOwnedBy(node2);
    }

    @After
    public void tearDown() throws IOException {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testLockOnClientCrash() throws InterruptedException {
        ILock lock = client1.getLock(keyOwnedByNode2);
        lock.lock();

        client1.getLifecycleService().terminate();

        lock = client2.getLock(keyOwnedByNode2);
        boolean lockObtained = lock.tryLock(120, TimeUnit.SECONDS);

        assertTrue("Lock was Not Obtained, lock should be released on client crash", lockObtained);
    }

    @Test
    public void testLockOnClient_withNodeCrash() throws InterruptedException {
        ILock lock = client1.getLock(keyOwnedByNode2);
        lock.lock();

        node2.getLifecycleService().terminate();

        lock = client2.getLock(keyOwnedByNode2);
        boolean lockObtained = lock.tryLock();

        assertFalse("Lock was obtained by 2 different clients ", lockObtained);
    }
}
