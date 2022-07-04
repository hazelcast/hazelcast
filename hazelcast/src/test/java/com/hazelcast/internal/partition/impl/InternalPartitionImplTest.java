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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionReplicaInterceptor;
import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.UuidUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InternalPartitionImplTest {

    private static final InetAddress LOCALHOST;

    static {
        try {
            LOCALHOST = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private final PartitionReplica localReplica = new PartitionReplica(newAddress(5000), UuidUtil.newUnsecureUUID());
    private final PartitionReplica[] replicaOwners = new PartitionReplica[MAX_REPLICA_COUNT];
    private final TestPartitionReplicaInterceptor partitionListener = new TestPartitionReplicaInterceptor();
    private InternalPartitionImpl partition;

    @Before
    public void setup() {
        partition = new InternalPartitionImpl(1, localReplica, partitionListener);
    }

    @Test
    public void testIsLocal_whenOwnedByThis() {
        replicaOwners[0] = localReplica;
        partition.setReplicas(replicaOwners);
        assertTrue(partition.isLocal());
    }

    @Test
    public void testIsLocal_whenNOTOwnedByThis() {
        replicaOwners[0] = new PartitionReplica(newAddress(6000), UuidUtil.newUnsecureUUID());
        partition.setReplicas(replicaOwners);
        assertFalse(partition.isLocal());
        assertEquals(1, partition.version());
    }

    @Test
    public void testGetOwnerOrNull_whenOwnerExists() {
        replicaOwners[0] = localReplica;
        partition.setReplicas(replicaOwners);
        assertEquals(localReplica, partition.getOwnerReplicaOrNull());
        assertEquals(localReplica.address(), partition.getOwnerOrNull());
    }

    @Test
    public void testGetOwnerOrNull_whenOwnerNOTExists() {
        assertNull(partition.getOwnerOrNull());
    }

    @Test
    public void testVersion_setReplicas() {
        replicaOwners[0] = localReplica;
        partition.setReplicas(replicaOwners);
        assertEquals(1, partition.version());

        PartitionReplica[] newReplicas = Arrays.copyOf(replicaOwners, replicaOwners.length);
        newReplicas[0] = new PartitionReplica(newAddress(6000), UuidUtil.newUnsecureUUID());
        newReplicas[1] = localReplica;
        partition.setReplicas(newReplicas);
        assertEquals(3, partition.version());
    }

    @Test
    public void testVersion_setReplica() {
        partition.setReplica(1, new PartitionReplica(newAddress(6000), UuidUtil.newUnsecureUUID()));
        assertEquals(1, partition.version());

        partition.setReplica(0, new PartitionReplica(newAddress(7000), UuidUtil.newUnsecureUUID()));
        assertEquals(2, partition.version());

        partition.setReplica(0, partition.getReplica(0));
        assertEquals(2, partition.version());
    }

    @Test
    public void testVersion_swapReplica() {
        partition.setReplica(1, new PartitionReplica(newAddress(6000), UuidUtil.newUnsecureUUID()));
        partition.setReplica(0, new PartitionReplica(newAddress(7000), UuidUtil.newUnsecureUUID()));

        partition.swapReplicas(1, 0);
        assertEquals(4, partition.version());
    }

    @Test
    public void testGetReplicaAddress() {
        replicaOwners[0] = localReplica;
        partition.setReplicas(replicaOwners);

        assertEquals(localReplica, partition.getReplica(0));
        assertEquals(localReplica.address(), partition.getReplicaAddress(0));
        for (int i = 1; i < MAX_REPLICA_COUNT; i++) {
            assertNull(partition.getReplica(i));
            assertNull(partition.getReplicaAddress(i));
        }
    }

    @Test
    public void testSetInitialReplicaAddresses() {
        for (int i = 0; i < replicaOwners.length; i++) {
            replicaOwners[i] = new PartitionReplica(newAddress(5000 + i), UuidUtil.newUnsecureUUID());
        }
        partition.setReplicas(replicaOwners);

        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            assertEquals(replicaOwners[i], partition.getReplica(i));
        }
    }

    @Test
    public void testSetReplicaAddresses() {
        for (int i = 0; i < replicaOwners.length; i++) {
            replicaOwners[i] = new PartitionReplica(newAddress(5000 + i), UuidUtil.newUnsecureUUID());
        }
        partition.setReplicas(replicaOwners);

        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            assertEquals(replicaOwners[i], partition.getReplica(i));
        }
    }

    @Test
    public void testSetReplicaAddresses_afterInitialSet() {
        replicaOwners[0] = localReplica;
        partition.setReplicas(replicaOwners);
        partition.setReplicas(replicaOwners);
    }

    @Test
    public void testSetReplicaAddresses_multipleTimes() {
        replicaOwners[0] = localReplica;
        partition.setReplicas(replicaOwners);
        partition.setReplicas(replicaOwners);
    }

    @Test
    public void testSetReplicaAddresses_ListenerShouldBeCalled() {
        replicaOwners[0] = localReplica;
        replicaOwners[1] = new PartitionReplica(newAddress(5001), UuidUtil.newUnsecureUUID());
        partition.setReplicas(replicaOwners);
        assertEquals(2, partitionListener.eventCount);
    }

    @Test
    public void testListenerShouldNOTBeCalled_whenReplicaRemainsSame() {
        replicaOwners[0] = localReplica;
        partition.setReplicas(replicaOwners);
        partitionListener.reset();

        partition.setReplicas(replicaOwners);
        assertEquals(0, partitionListener.eventCount);
    }

    @Test
    public void testIsOwnerOrBackup() {
        replicaOwners[0] = localReplica;
        Address otherAddress = newAddress(5001);
        replicaOwners[1] = new PartitionReplica(otherAddress, UuidUtil.newUnsecureUUID());
        partition.setReplicas(replicaOwners);

        assertTrue(partition.isOwnerOrBackup(replicaOwners[0]));
        assertTrue(partition.isOwnerOrBackup(localReplica));
        assertTrue(partition.isOwnerOrBackup(replicaOwners[1]));
        assertTrue(partition.isOwnerOrBackup(otherAddress));
        assertFalse(partition.isOwnerOrBackup(new PartitionReplica(newAddress(6000), UuidUtil.newUnsecureUUID())));
        assertFalse(partition.isOwnerOrBackup(newAddress(6000)));
    }

    @Test
    public void testGetReplicaIndex() {
        replicaOwners[0] = localReplica;
        replicaOwners[1] = new PartitionReplica(newAddress(5001), UuidUtil.newUnsecureUUID());
        partition.setReplicas(replicaOwners);

        assertEquals(0, partition.getReplicaIndex(replicaOwners[0]));
        assertEquals(1, partition.getReplicaIndex(replicaOwners[1]));
        assertEquals(-1, partition.getReplicaIndex(new PartitionReplica(newAddress(6000), UuidUtil.newUnsecureUUID())));
    }

    @Test
    public void testReset() {
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            replicaOwners[i] = new PartitionReplica(newAddress(5000 + i), UuidUtil.newUnsecureUUID());
        }
        partition.setReplicas(replicaOwners);

        partition.reset(localReplica);
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            assertNull(partition.getReplicaAddress(i));
        }
        assertFalse(partition.isMigrating());
        assertEquals(0, partition.version());
    }

    private static Address newAddress(int port) {
        return new Address("127.0.0.1", LOCALHOST, 5000 + port);
    }

    private static class TestPartitionReplicaInterceptor implements PartitionReplicaInterceptor {
        private int eventCount;

        @Override
        public void replicaChanged(int partitionId, int replicaIndex, PartitionReplica oldReplica, PartitionReplica newReplica) {
            eventCount++;
        }

        void reset() {
            eventCount = 0;
        }
    }
}
