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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.function.BiConsumer;
import com.hazelcast.util.function.Consumer;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;


@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientLockRetryWhenOwnerDiesTest extends ClientTestSupport {

    private static long leaseTime = 120;
    private static long waitTime = 120;

    static class Shutdown implements Consumer<HazelcastInstance> {
        @Override
        public void accept(HazelcastInstance hazelcastInstance) {
            hazelcastInstance.shutdown();
        }

        @Override
        public String toString() {
            return "Shutdown{}";
        }
    }

    static class Terminate implements Consumer<HazelcastInstance> {
        @Override
        public void accept(HazelcastInstance hazelcastInstance) {
            hazelcastInstance.getLifecycleService().terminate();
        }

        @Override
        public String toString() {
            return "Terminate{}";
        }
    }

    static class LockLock implements BiConsumer<HazelcastInstance, String> {

        @Override
        public void accept(HazelcastInstance client, String key) {
            ILock lock = client.getLock(key);
            lock.lock();
            lock.unlock();
        }

        @Override
        public String toString() {
            return "LockLock{}";
        }
    }

    static class LockLockLease implements BiConsumer<HazelcastInstance, String> {

        @Override
        public void accept(HazelcastInstance client, String key) {
            ILock lock = client.getLock(key);
            lock.lock(leaseTime, TimeUnit.SECONDS);
            lock.unlock();
        }

        @Override
        public String toString() {
            return "LockLockLease{}";
        }
    }

    static class LockTryLockTimeout implements BiConsumer<HazelcastInstance, String> {

        @Override
        public void accept(HazelcastInstance client, String key) {
            ILock lock = client.getLock(key);
            try {
                lock.tryLock(waitTime, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            lock.unlock();
        }

        @Override
        public String toString() {
            return "LockTryLockTimeout{}";
        }
    }

    static class LockTryLockTimeoutLease implements BiConsumer<HazelcastInstance, String> {

        @Override
        public void accept(HazelcastInstance client, String key) {
            ILock lock = client.getLock(key);
            try {
                lock.tryLock(waitTime, TimeUnit.SECONDS, leaseTime, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            lock.unlock();
        }

        @Override
        public String toString() {
            return "LockTryLockTimeoutLease{}";
        }
    }

    static class MapLock implements BiConsumer<HazelcastInstance, String> {

        @Override
        public void accept(HazelcastInstance client, String key) {
            IMap map = client.getMap(key);
            map.lock(key);
            map.unlock(key);
        }

        @Override
        public String toString() {
            return "MapLock{}";
        }
    }

    static class MapLockLease implements BiConsumer<HazelcastInstance, String> {

        @Override
        public void accept(HazelcastInstance client, String key) {
            IMap map = client.getMap(key);
            map.lock(key, leaseTime, TimeUnit.SECONDS);
            map.unlock(key);
        }
    }

    static class MapTryLockTimeout implements BiConsumer<HazelcastInstance, String> {

        @Override
        public void accept(HazelcastInstance client, String key) {
            IMap map = client.getMap(key);
            try {
                map.tryLock(key, waitTime, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            map.unlock(key);
        }

        @Override
        public String toString() {
            return "MapTryLockTimeout{}";
        }
    }

    static class MapTryLockTimeoutLease implements BiConsumer<HazelcastInstance, String> {

        @Override
        public void accept(HazelcastInstance client, String key) {
            IMap map = client.getMap(key);
            try {
                map.tryLock(key, waitTime, TimeUnit.SECONDS, leaseTime, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            map.unlock(key);
        }

        @Override
        public String toString() {
            return "MapTryLockTimeoutLease{}";
        }
    }


    @Parameterized.Parameters(name = "closePolicy:{0}, lockPolicy:{1}")
    public static Collection<Object[]> parameters() {
        List<Consumer<HazelcastInstance>> closePolicies = asList(new Shutdown(), new Terminate());
        List<BiConsumer<HazelcastInstance, String>> lockPolicies =
                asList(new LockLock(), new LockLockLease(), new LockTryLockTimeout(), new LockTryLockTimeoutLease(),
                        new MapLock(), new MapLockLease(), new MapTryLockTimeout(), new MapTryLockTimeoutLease());

        Collection<Object[]> objects = new LinkedList<Object[]>();
        for (Consumer<HazelcastInstance> closePolicy : closePolicies) {
            for (BiConsumer<HazelcastInstance, String> lockPolicy : lockPolicies) {
                objects.add(new Object[]{closePolicy, lockPolicy});
            }
        }

        return objects;
    }

    @Parameterized.Parameter
    public Consumer<HazelcastInstance> closePolicy;

    @Parameterized.Parameter(1)
    public BiConsumer<HazelcastInstance, String> lockPolicy;


    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void teardown() {
        factory.terminateAll();
    }

    @Test
    public void testKeyOwnerCloses_afterInvocationTimeout() throws Exception {
        HazelcastInstance keyOwner = factory.newHazelcastInstance();
        HazelcastInstance instance = factory.newHazelcastInstance();
        warmUpPartitions(keyOwner, instance);

        ClientConfig clientConfig = new ClientConfig();
        long invocationTimeoutMillis = 1000;
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(),
                String.valueOf(TimeUnit.MILLISECONDS.toSeconds(invocationTimeoutMillis)));
        final HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        //this is needed in the test because we set the timeout too short for faster test.
        makeSureConnectedToServers(client, 2);

        final String key = generateKeyOwnedBy(keyOwner);
        ILock lock = client.getLock(key);
        lock.lock();

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                lockPolicy.accept(client, key);
                latch.countDown();
            }
        }).start();

        Thread.sleep(invocationTimeoutMillis * 2);
        closePolicy.accept(keyOwner);

        //wait for the key owned by second member after close to avoid operation timeout during transition
        //this is needed in the test because we set the timeout too short for faster test.
        Member secondMember = instance.getCluster().getLocalMember();
        while (!secondMember.equals(client.getPartitionService().getPartition(key).getOwner())) {
            Thread.sleep(100);
        }

        assertTrue(lock.isLocked());
        assertTrue(lock.isLockedByCurrentThread());
        assertTrue(lock.tryLock());
        lock.unlock();
        lock.unlock();
        assertOpenEventually(latch);
    }

}


