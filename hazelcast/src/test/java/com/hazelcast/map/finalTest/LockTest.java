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

package com.hazelcast.map.finalTest;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.instance.StaticNodeFactory;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LockTest {

    @Test
    public void testBackupDies() throws TransactionException {
        Config config = new Config();
        StaticNodeFactory factory = new StaticNodeFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map1 = h1.getMap("default");
        final int size = 50;
        final CountDownLatch latch = new CountDownLatch(size+1);

        Runnable runnable = new Runnable() {
            public void run() {
                for (int i = 0; i < size; i++) {
                    map1.lock(i);
                    System.out.println("turn:"+i);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                    }
                    latch.countDown();
                }
                for (int i = 0; i < size; i++) {
                    assertTrue(map1.isLocked(i));
                }
                for (int i = 0; i < size; i++) {
                    map1.unlock(i);
                }
                for (int i = 0; i < size; i++) {
                    assertFalse(map1.isLocked(i));
                }
                latch.countDown();
            }
        };
        new Thread(runnable).start();
        try {
            Thread.sleep(1000);
            System.out.println("SHUTTING DOWNN");
            h2.getLifecycleService().shutdown();
            latch.await();
            for (int i = 0; i < size; i++) {
                assertFalse(map1.isLocked(i));
            }
        } catch (InterruptedException e) {
        }
    }


}
