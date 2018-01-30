/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.partition.PartitionListener;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InternalPartitionImplTest {

    private static final InetAddress LOCALHOST;

    static {
        try {
            LOCALHOST = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private final Address thisAddress = newAddress(5000);
    private final Address[] replicaAddresses = new Address[MAX_REPLICA_COUNT];
    private final TestPartitionListener partitionListener = new TestPartitionListener();
    private InternalPartitionImpl partition;

    @Before
    public void setup() {
        partition = new InternalPartitionImpl(1, partitionListener, thisAddress);
    }

    @Test
    public void testIsLocal_whenOwnedByThis() throws Exception {
        replicaAddresses[0] = thisAddress;
        partition.setInitialReplicaAddresses(replicaAddresses);
        assertTrue(partition.isLocal());
    }

    @Test
    public void testIsLocal_whenNOTOwnedByThis() throws Exception {
        replicaAddresses[0] = newAddress(6000);
        partition.setInitialReplicaAddresses(replicaAddresses);
        assertFalse(partition.isLocal());
    }

    @Test
    public void testGetOwnerOrNull_whenOwnerExists() throws Exception {
        replicaAddresses[0] = thisAddress;
        partition.setInitialReplicaAddresses(replicaAddresses);
        assertEquals(thisAddress, partition.getOwnerOrNull());
    }

    @Test
    public void testGetOwnerOrNull_whenOwnerNOTExists() throws Exception {
        assertNull(partition.getOwnerOrNull());
    }

    @Test
    public void testGetReplicaAddress() throws Exception {
        replicaAddresses[0] = thisAddress;
        partition.setInitialReplicaAddresses(replicaAddresses);

        assertEquals(thisAddress, partition.getReplicaAddress(0));
        for (int i = 1; i < MAX_REPLICA_COUNT; i++) {
            assertNull(partition.getReplicaAddress(i));
        }
    }

    @Test
    public void testSetInitialReplicaAddresses() throws Exception {
        for (int i = 0; i < replicaAddresses.length; i++) {
            replicaAddresses[i] = newAddress(5000 + i);
        }
        partition.setInitialReplicaAddresses(replicaAddresses);

        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            assertEquals(replicaAddresses[i], partition.getReplicaAddress(i));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testSetInitialReplicaAddresses_multipleTimes() throws Exception {
        replicaAddresses[0] = thisAddress;
        partition.setInitialReplicaAddresses(replicaAddresses);
        partition.setInitialReplicaAddresses(replicaAddresses);
    }

    @Test
    public void testSetInitialReplicaAddresses_ListenerShouldNOTBeCalled() throws Exception {
        replicaAddresses[0] = thisAddress;
        partition.setInitialReplicaAddresses(replicaAddresses);
        assertEquals(0, partitionListener.eventCount);
    }

    @Test
    public void testSetReplicaAddresses() throws Exception {
        for (int i = 0; i < replicaAddresses.length; i++) {
            replicaAddresses[i] = newAddress(5000 + i);
        }
        partition.setReplicaAddresses(replicaAddresses);

        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            assertEquals(replicaAddresses[i], partition.getReplicaAddress(i));
        }
    }

    @Test
    public void testSetReplicaAddresses_afterInitialSet() throws Exception {
        replicaAddresses[0] = thisAddress;
        partition.setInitialReplicaAddresses(replicaAddresses);
        partition.setReplicaAddresses(replicaAddresses);
    }

    @Test
    public void testSetReplicaAddresses_multipleTimes() throws Exception {
        replicaAddresses[0] = thisAddress;
        partition.setReplicaAddresses(replicaAddresses);
        partition.setReplicaAddresses(replicaAddresses);
    }

    @Test
    public void testSetReplicaAddresses_ListenerShouldBeCalled() throws Exception {
        replicaAddresses[0] = thisAddress;
        replicaAddresses[1] = newAddress(5001);
        partition.setReplicaAddresses(replicaAddresses);
        assertEquals(2, partitionListener.eventCount);
    }

    @Test
    public void testListenerShouldNOTBeCalled_whenReplicaRemainsSame() throws Exception {
        replicaAddresses[0] = thisAddress;
        partition.setReplicaAddresses(replicaAddresses);
        partitionListener.reset();

        partition.setReplicaAddresses(replicaAddresses);
        assertEquals(0, partitionListener.eventCount);
    }

    @Test
    public void testIsOwnerOrBackup() throws Exception {
        replicaAddresses[0] = thisAddress;
        Address otherAddress = newAddress(5001);
        replicaAddresses[1] = otherAddress;
        partition.setReplicaAddresses(replicaAddresses);

        assertTrue(partition.isOwnerOrBackup(thisAddress));
        assertTrue(partition.isOwnerOrBackup(otherAddress));
        assertFalse(partition.isOwnerOrBackup(newAddress(6000)));
    }

    @Test
    public void testGetReplicaIndex() throws Exception {
        replicaAddresses[0] = thisAddress;
        Address otherAddress = newAddress(5001);
        replicaAddresses[1] = otherAddress;
        partition.setReplicaAddresses(replicaAddresses);

        assertEquals(0, partition.getReplicaIndex(thisAddress));
        assertEquals(1, partition.getReplicaIndex(otherAddress));
        assertEquals(-1, partition.getReplicaIndex(newAddress(6000)));
    }

    @Test
    public void testReset() throws Exception {
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            replicaAddresses[i] = newAddress(5000 + i);
        }
        partition.setReplicaAddresses(replicaAddresses);

        partition.reset();
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            assertNull(partition.getReplicaAddress(i));
        }
        assertFalse(partition.isMigrating());
    }

    private static Address newAddress(int port) {
        return new Address("127.0.0.1", LOCALHOST, 5000 + port);
    }

    private static class TestPartitionListener implements PartitionListener {
        private int eventCount;

        @Override
        public void replicaChanged(PartitionReplicaChangeEvent event) {
            eventCount++;
        }

        void reset() {
            eventCount = 0;
        }
    }
}
