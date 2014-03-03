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
import com.hazelcast.test.annotation.QuickTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientLockWithClusterProblemsTest {

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
    public void clear() throws IOException {
        Hazelcast.shutdownAll();
    }


    @Test
    public void testObtainLockTwice() throws InterruptedException {

        ILock lock = client1.getLock(keyOwnedByNode1);
        lock.lock();

        lock = client2.getLock(keyOwnedByNode1);

        boolean lockObtained = lock.tryLock(5, TimeUnit.SECONDS);

        assertFalse("Lock obtained by 2 client ", lockObtained);
    }


    @Test
    public void testLockOnClientCrash() throws InterruptedException {

        ILock lock = client1.getLock(keyOwnedByNode1);
        lock.lock();

        client1.getLifecycleService().terminate();

        lock = client2.getLock("a");

        boolean lockObtained = lock.tryLock(5, TimeUnit.SECONDS);

        assertTrue("Lock was Not Obtained after 5 seconds lock should be released on client1 crash", lockObtained);
    }


    @Test
    public void testLockOnClient_withNodeCrash() throws InterruptedException {

        ILock lock = client1.getLock(keyOwnedByNode1);
        lock.lock();

        node1.getLifecycleService().terminate();

        lock = client2.getLock(keyOwnedByNode1);

        boolean lockObtained = lock.tryLock(6, TimeUnit.SECONDS);

        assertFalse("Lock was obtained by 2 diffrent clients ", lockObtained);
    }
}
