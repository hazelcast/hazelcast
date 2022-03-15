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
import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MigrationPlannerCycleTest {

    private MigrationPlanner migrationPlanner = new MigrationPlanner();
    private UUID[] uuids = {
            new UUID(57, 1),
            new UUID(57, 2),
            new UUID(57, 3),
            new UUID(57, 4),
            new UUID(57, 5),
            new UUID(57, 6),
            new UUID(57, 7)
    };

    @Test
    public void testCycle1() throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                null,
                null,
                null,
                null,
                null,
        };
        final PartitionReplica[] newReplicas = {
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                null,
                null,
                null,
                null,
                null,
        };

        assertTrue(migrationPlanner.isCyclic(oldReplicas, newReplicas));
    }

    @Test
    public void testCycle1_fixed() throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                null,
                null,
                null,
                null,
                null,
        };
        final PartitionReplica[] newReplicas = new PartitionReplica[] {
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                null,
                null,
                null,
                null,
                null,
        };

        assertTrue(migrationPlanner.fixCycle(oldReplicas, newReplicas));
        assertArrayEquals(oldReplicas, newReplicas);
    }

    @Test
    public void testCycle2() throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                null,
                null,
                null,
                null,
        };
        final PartitionReplica[] newReplicas = {
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                null,
                null,
                null,
                null,
        };

        assertTrue(migrationPlanner.isCyclic(oldReplicas, newReplicas));
    }

    @Test
    public void testCycle2_fixed() throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                null,
                null,
                null,
                null,
        };
        final PartitionReplica[] newReplicas = {
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                null,
                null,
                null,
                null,
        };

        assertTrue(migrationPlanner.fixCycle(oldReplicas, newReplicas));
        assertArrayEquals(oldReplicas, newReplicas);
    }

    @Test
    public void testCycle3() throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                new PartitionReplica(new Address("localhost", 5705), uuids[4]),
                null,
                null,
        };
        final PartitionReplica[] newReplicas = {
                new PartitionReplica(new Address("localhost", 5705), uuids[4]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                null,
                null,
        };

        assertTrue(migrationPlanner.isCyclic(oldReplicas, newReplicas));
    }

    @Test
    public void testCycle3_fixed() throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                new PartitionReplica(new Address("localhost", 5705), uuids[4]),
                null,
                null,
        };
        final PartitionReplica[] newReplicas = {
                new PartitionReplica(new Address("localhost", 5705), uuids[4]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                null,
                null,
        };

        assertTrue(migrationPlanner.fixCycle(oldReplicas, newReplicas));
        assertArrayEquals(oldReplicas, newReplicas);
    }

    @Test
    public void testCycle4() throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                new PartitionReplica(new Address("localhost", 5705), uuids[4]),
                new PartitionReplica(new Address("localhost", 5706), uuids[5]),
                new PartitionReplica(new Address("localhost", 5707), uuids[6]),
        };
        final PartitionReplica[] newReplicas = {
                new PartitionReplica(new Address("localhost", 5705), uuids[4]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5707), uuids[6]),
                new PartitionReplica(new Address("localhost", 5706), uuids[5]),
        };

        assertTrue(migrationPlanner.isCyclic(oldReplicas, newReplicas));
    }

    @Test
    public void testCycle4_fixed() throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                new PartitionReplica(new Address("localhost", 5705), uuids[4]),
                new PartitionReplica(new Address("localhost", 5706), uuids[5]),
                new PartitionReplica(new Address("localhost", 5707), uuids[6]),
        };
        final PartitionReplica[] newReplicas = {
                new PartitionReplica(new Address("localhost", 5705), uuids[4]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5707), uuids[6]),
                new PartitionReplica(new Address("localhost", 5706), uuids[5]),
        };

        assertTrue(migrationPlanner.fixCycle(oldReplicas, newReplicas));
        assertArrayEquals(oldReplicas, newReplicas);
    }

    @Test
    public void testNoCycle() throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                null,
                null,
                null,
                null,
                null,
        };
        final PartitionReplica[] newReplicas = {
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                null,
                null,
                null,
                null,
                null,
        };

        assertFalse(migrationPlanner.isCyclic(oldReplicas, newReplicas));
    }

    @Test
    public void testNoCycle2() throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                new PartitionReplica(new Address("localhost", 5705), uuids[4]),
                null,
                null,
        };
        final PartitionReplica[] newReplicas = {
                new PartitionReplica(new Address("localhost", 5706), uuids[5]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                null,
                null,
        };

        assertFalse(migrationPlanner.isCyclic(oldReplicas, newReplicas));
    }
}
