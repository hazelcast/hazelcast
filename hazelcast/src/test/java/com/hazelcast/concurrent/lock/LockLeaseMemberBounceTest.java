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

package com.hazelcast.concurrent.lock;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.jitter.JitterRule;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Random;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.generateKeyNotOwnedBy;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Runs a cluster of {@value MEMBER_COUNT + 1} and calls lease lock from {@value DRIVER_COUNT} drivers. During the test bounces
 * {@value MEMBER_COUNT} cluster members (shuts down and restarts). The remaining 1 member is stable (never bounced) but no
 * locks are owned by this member.
 * Asserts that all locks are unlocked eventually.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class LockLeaseMemberBounceTest {
    private static final int MEMBER_COUNT = 4;
    private static final int DRIVER_COUNT = 4;
    private static final int LEASE_DURATION_MILLIS = 100;
    private static final int TEST_DURATION_SECONDS = 40;
    private static final int LOCK_COUNT = 500;
    private static final int CONCURRENCY = 2;

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(getConfig())
            .clusterSize(MEMBER_COUNT)
            .driverCount(DRIVER_COUNT).build();
    @Rule
    public JitterRule jitterRule = new JitterRule();

    @Test
    public void leaseShouldExpireWhenMemberBouncing() {
        final HazelcastInstance steadyMember = bounceMemberRule.getSteadyMember();

        String[] lockNames = new String[LOCK_COUNT];
        for (int i = 0; i < lockNames.length; i++) {
            lockNames[i] = generateKeyNotOwnedBy(steadyMember);
        }

        final Driver[] drivers = new Driver[DRIVER_COUNT];
        for (int i = 0; i < drivers.length; i++) {
            drivers[i] = new Driver(bounceMemberRule.getNextTestDriver(), lockNames);
        }

        DriverRunnable[] runnables = new DriverRunnable[drivers.length * CONCURRENCY];
        for (int i = 0; i < drivers.length; i++) {
            for (int j = 0; j < CONCURRENCY; j++) {
                runnables[i * CONCURRENCY + j] = new DriverRunnable(drivers[i]);
            }
        }

        bounceMemberRule.testRepeatedly(runnables, TEST_DURATION_SECONDS);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (Driver driver : drivers) {
                    driver.doAssert();
                }
            }
        }, MILLISECONDS.toSeconds(LEASE_DURATION_MILLIS) + 10);
    }

    protected Config getConfig() {
        return new Config();
    }


    public static class DriverRunnable implements Runnable {
        private final Driver client;

        DriverRunnable(Driver client) {
            this.client = client;
        }

        @Override
        public void run() {
            final ILock randomLock = client.getRandomLock();
            randomLock.lock(LEASE_DURATION_MILLIS, MILLISECONDS);
        }
    }

    public static class Driver {
        private final ILock[] locks;
        private Random random = new Random();

        Driver(HazelcastInstance hz, String[] lockNames) {
            locks = new ILock[lockNames.length];
            for (int i = 0; i < locks.length; i++) {
                locks[i] = hz.getLock(lockNames[i]);
            }
        }

        ILock getRandomLock() {
            return locks[random.nextInt(locks.length)];
        }

        void doAssert() {
            for (ILock lock : locks) {
                Assert.assertFalse(lock.isLocked());
            }
        }

        HashSet<ILock> getAllLocked() {
            final HashSet<ILock> locked = new HashSet<ILock>();
            for (ILock lock : locks) {
                if (lock.isLocked()) {
                    locked.add(lock);
                }
            }
            return locked;
        }
    }
}
