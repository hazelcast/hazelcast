/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientLockWithTerminationTest {

    private HazelcastInstance node1;
    private HazelcastInstance node2;
    private HazelcastInstance client1;
    private HazelcastInstance client2;
    private String keyOwnedByNode1;

    @Before
    public void setup() throws InterruptedException {
        node1 = Hazelcast.newHazelcastInstance();
        node2 = Hazelcast.newHazelcastInstance();
        client1 = HazelcastClient.newHazelcastClient();
        client2 = HazelcastClient.newHazelcastClient();
        keyOwnedByNode1 = HazelcastTestSupport.generateKeyOwnedBy(node1);
    }

    @After
    public void tearDown() throws IOException {
        node1.shutdown();
        node2.shutdown();
        client1.shutdown();
        client2.shutdown();
        Hazelcast.shutdownAll();
        HazelcastClient.shutdownAll();
    }

    @Test
    public void testLockOnClient_withClientCrash() throws InterruptedException {
        ILock lock = client1.getLock(keyOwnedByNode1);
        lock.lock();

        client1.getLifecycleService().terminate();

        lock = client2.getLock(keyOwnedByNode1);
        boolean lockObtained = lock.tryLock();

        assertTrue("Lock was Not Obtained, lock should be released on client crash", lockObtained);
    }

    @Test
    @Category(ProblematicTest.class)
    public void testLockOnClient_withNodeCrash() throws InterruptedException {
        ILock lock = client1.getLock(keyOwnedByNode1);
        lock.lock();

        node1.getLifecycleService().terminate();

        lock = client2.getLock(keyOwnedByNode1);
        boolean lockObtained = lock.tryLock();

        assertFalse("Lock was obtained by 2 different clients ", lockObtained);
    }
}
