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

    @Before
    @After
    public void clear() throws IOException {
        Hazelcast.shutdownAll();
    }


    @Test
    public void testObtainLockTwice() throws InterruptedException {

        final HazelcastInstance node1 = Hazelcast.newHazelcastInstance();

        final HazelcastInstance client1 = HazelcastClient.newHazelcastClient();
        final HazelcastInstance client2 = HazelcastClient.newHazelcastClient();

        ILock lock = client1.getLock("a");

        lock.lock();

        lock = client2.getLock("a");

        if ( lock.tryLock(5, TimeUnit.SECONDS) ) {

            fail("Failed same Lock was obtained by 2 diffrent clients");
        }
    }

    @Test
    public void testLockOnClientCrash() throws InterruptedException {

        final HazelcastInstance node1 = Hazelcast.newHazelcastInstance();

        final HazelcastInstance client1 = HazelcastClient.newHazelcastClient();
        final HazelcastInstance client2 = HazelcastClient.newHazelcastClient();


        ILock lock = client1.getLock("a");
        lock.lock();

        client1.getLifecycleService().terminate();

        lock = client2.getLock("a");

        if ( lock.tryLock(5, TimeUnit.SECONDS) ) {

            lock.unlock();

        } else {
            fail("Failed to obtain lock after 5 seconds lock should be released on client1 crash");
        }
    }



    @Test
    public void testLockOnClient_withNodeCrash() throws InterruptedException {

        final HazelcastInstance node1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance node2 = Hazelcast.newHazelcastInstance();


        final HazelcastInstance client1 = HazelcastClient.newHazelcastClient();
        final HazelcastInstance client2 = HazelcastClient.newHazelcastClient();


        ILock lock = client1.getLock("a");
        lock.lock();

        node2.getLifecycleService().terminate();

        lock = client2.getLock("a");

        if ( lock.tryLock(6, TimeUnit.SECONDS) ) {

            fail("Failed same Lock was obtained by 2 diffrent clients");

        }
    }


}
