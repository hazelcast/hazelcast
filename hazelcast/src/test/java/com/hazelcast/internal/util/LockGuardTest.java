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

package com.hazelcast.internal.util;

import com.hazelcast.cluster.Address;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LockGuardTest {
    private static final UUID TXN = UUID.randomUUID();
    private static final UUID ANOTHER_TXN = UUID.randomUUID();

    @Test
    public void testNotLocked() {
        LockGuard stateLock = LockGuard.NOT_LOCKED;
        assertNull(stateLock.getLockOwner());
        assertEquals(0L, stateLock.getLockExpiryTime());
    }

    @Test(expected = NullPointerException.class)
    public void testAllowsLock_nullTransactionId() throws Exception {
        LockGuard stateLock = LockGuard.NOT_LOCKED;
        stateLock.allowsLock(null);
    }

    @Test(expected = NullPointerException.class)
    public void testAllowsUnlock_nullTransactionId() throws Exception {
        LockGuard stateLock = LockGuard.NOT_LOCKED;
        stateLock.allowsUnlock(null);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_nullEndpoint() throws Exception {
        new LockGuard(null, TXN, 1000);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_nullTransactionId() throws Exception {
        Address endpoint = newAddress();
        new LockGuard(endpoint, null, 1000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_nonPositiveLeaseTime() throws Exception {
        Address endpoint = newAddress();
        new LockGuard(endpoint, TXN, -1000);
    }

    @Test
    public void testAllowsLock_success() throws Exception {
        LockGuard stateLock = LockGuard.NOT_LOCKED;
        assertTrue(stateLock.allowsLock(TXN));
    }

    @Test
    public void testAllowsLock_fail() throws Exception {
        Address endpoint = newAddress();
        LockGuard stateLock = new LockGuard(endpoint, TXN, 1000);
        assertFalse(stateLock.allowsLock(ANOTHER_TXN));
    }

    @Test
    public void testIsLocked() throws Exception {
        LockGuard stateLock = LockGuard.NOT_LOCKED;
        assertFalse(stateLock.isLocked());

        Address endpoint = newAddress();
        stateLock = new LockGuard(endpoint, TXN, 1000);
        assertTrue(stateLock.isLocked());
    }

    @Test
    public void testIsLeaseExpired() throws Exception {
        LockGuard stateLock = LockGuard.NOT_LOCKED;
        assertFalse(stateLock.isLeaseExpired());

        Address endpoint = newAddress();
        stateLock = new LockGuard(endpoint, TXN, TimeUnit.HOURS.toMillis(1));
        assertFalse(stateLock.isLeaseExpired());

        stateLock = new LockGuard(endpoint, TXN, 1);
        final LockGuard finalStateLock = stateLock;
        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(finalStateLock.isLeaseExpired());
            }
        });
    }

    private Address newAddress() throws UnknownHostException {
        return new Address(InetAddress.getLocalHost(), 5000);
    }
}
