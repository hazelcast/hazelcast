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

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.MigrationPlanner.MigrationDecisionCallback;
import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.UuidUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MigrationPlannerTest {

    private MigrationDecisionCallback callback = mock(MigrationDecisionCallback.class);
    private MigrationPlanner migrationPlanner = new MigrationPlanner();
    private UUID[] uuids = { new UUID(57, 1),
            new UUID(57, 2),
            new UUID(57, 3),
            new UUID(57, 4),
            new UUID(57, 5),
            new UUID(57, 6),
            new UUID(57, 7)
    };


    @Test
    public void test_MOVE() throws UnknownHostException {
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
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5705), uuids[4]),
                null,
                null,
                null,
                null,
        };

        migrationPlanner.planMigrations(0, oldReplicas, newReplicas, callback);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5701), uuids[0]), 0, -1, new PartitionReplica(new Address("localhost", 5704), uuids[3]), -1, 0);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5703), uuids[2]), 2, -1, new PartitionReplica(new Address("localhost", 5705), uuids[4]), -1, 2);
    }

    @Test
    public void test_COPY() throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                null,
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                null,
                null,
                null,
                null,
        };
        final PartitionReplica[] newReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                null,
                null,
                null,
                null,
        };

        migrationPlanner.planMigrations(0, oldReplicas, newReplicas, callback);
        verify(callback).migrate(null, -1, -1, new PartitionReplica(new Address("localhost", 5704), uuids[3]), -1, 1);
    }

    @Test
    public void test_SHIFT_DOWN_withNullKeepReplicaIndex() throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                null,
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                null,
                null,
                null,
                null,
        };
        final PartitionReplica[] newReplicas = {
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                null,
                null,
                null,
                null,
        };

        migrationPlanner.planMigrations(0, oldReplicas, newReplicas, callback);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5701), uuids[0]), 0, 1, new PartitionReplica(new Address("localhost", 5704), uuids[3]), -1, 0);
    }

    @Test
    public void test_SHIFT_DOWN_withNullNonNullKeepReplicaIndex() throws UnknownHostException {
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
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                null,
                null,
                null,
                null,
        };

        migrationPlanner.planMigrations(0, oldReplicas, newReplicas, callback);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5701), uuids[0]), 0, -1, new PartitionReplica(new Address("localhost", 5704), uuids[3]), -1, 0);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5702), uuids[1]), 1, -1, new PartitionReplica(new Address("localhost", 5701), uuids[0]), -1, 1);
    }

    @Test
    public void test_SHIFT_DOWN_performedBy_MOVE() throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                null,
                null,
                null,
                null,
        };
        final PartitionReplica[] newReplicas = {new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                                                 new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                                                 new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 };

        migrationPlanner.planMigrations(0, oldReplicas, newReplicas, callback);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5701), uuids[0]), 0, -1, new PartitionReplica(new Address("localhost", 5704), uuids[3]), -1, 0);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5702), uuids[1]), 1, -1, new PartitionReplica(new Address("localhost", 5701), uuids[0]), -1, 1);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5703), uuids[2]), 2, -1, new PartitionReplica(new Address("localhost", 5702), uuids[1]), -1, 2);
    }

    @Test
    public void test_SHIFT_UP() throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                null,
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                null,
                null,
                null,
        };
        final PartitionReplica[] newReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                null,
                null,
                null,
                null,
        };

        migrationPlanner.planMigrations(0, oldReplicas, newReplicas, callback);

        verify(callback).migrate(null, -1, -1, new PartitionReplica(new Address("localhost", 5703), uuids[2]), 2, 1);
        verify(callback).migrate(null, -1, -1, new PartitionReplica(new Address("localhost", 5704), uuids[3]), 3, 2);
    }

    @Test
    public void test_SHIFT_UPS_performedBy_MOVE() throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                null,
                null,
                null,
        };
        final PartitionReplica[] newReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                new PartitionReplica(new Address("localhost", 5705), uuids[4]),
                null,
                null,
                null,
        };

        migrationPlanner.planMigrations(0, oldReplicas, newReplicas, callback);

        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5704), uuids[3]), 3, -1, new PartitionReplica(new Address("localhost", 5705), uuids[4]), -1, 3);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5703), uuids[2]), 2, -1, new PartitionReplica(new Address("localhost", 5704), uuids[3]), -1, 2);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5702), uuids[1]), 1, -1, new PartitionReplica(new Address("localhost", 5703), uuids[2]), -1, 1);
    }

    @Test
    public void test_SHIFT_DOWN_performedAfterKnownNewReplicaOwnerKickedOutOfReplicas() throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5705), uuids[4]),
                null,
                null,
                null,
        };
        final PartitionReplica[] newReplicas = {
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5705), uuids[4]),
                new PartitionReplica(new Address("localhost", 5706), uuids[5]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                null,
        };

        migrationPlanner.planMigrations(0, oldReplicas, newReplicas, callback);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5701), uuids[0]), 0, 5, new PartitionReplica(new Address("localhost", 5704), uuids[3]), -1, 0);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5705), uuids[4]), 3, -1, new PartitionReplica(new Address("localhost", 5706), uuids[5]), -1, 3);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5703), uuids[2]), 2, -1, new PartitionReplica(new Address("localhost", 5705), uuids[4]), -1, 2);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5702), uuids[1]), 1, 4, new PartitionReplica(new Address("localhost", 5703), uuids[2]), -1, 1);
    }

    @Test
    public void test_SHIFT_DOWN_performedBeforeNonConflicting_SHIFT_UP() throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5705), uuids[4]),
                null,
                null,
                null,
        };
        final PartitionReplica[] newReplicas = {
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5705), uuids[4]),
                new PartitionReplica(new Address("localhost", 5706), uuids[5]),
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                null,
                null,
        };

        migrationPlanner.planMigrations(0, oldReplicas, newReplicas, callback);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5701), uuids[0]), 0, 4, new PartitionReplica(new Address("localhost", 5704), uuids[3]), -1, 0);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5705), uuids[4]), 3, -1, new PartitionReplica(new Address("localhost", 5706), uuids[5]), -1, 3);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5703), uuids[2]), 2, -1, new PartitionReplica(new Address("localhost", 5705), uuids[4]), -1, 2);
    }

    @Test
    public void test_MOVE_toNull() throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5705), uuids[4]),
                null,
                null,
                null,
        };
        final PartitionReplica[] newReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                null,
                null,
                null,
                null,
        };

        migrationPlanner.planMigrations(0, oldReplicas, newReplicas, callback);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5705), uuids[4]), 3, -1, null, -1, -1);
    }

    @Test
    public void test_SHIFT_UP_toReplicaIndexWithExistingOwner() throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                null,
                null,
                null,
        };
        final PartitionReplica[] newReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                null,
                null,
                null,
                null,
        };

        migrationPlanner.planMigrations(0, oldReplicas, newReplicas, callback);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5702), uuids[1]), 1, -1, new PartitionReplica(new Address("localhost", 5704), uuids[3]), 3, 1);
    }

    @Test
    public void test_MOVE_performedAfter_SHIFT_UP_toReplicaIndexWithExistingOwnerKicksItOutOfCluster()
            throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                null,
                null,
                null,
        };
        final PartitionReplica[] newReplicas = {
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                null,
                null,
                null,
                null,
        };

        migrationPlanner.planMigrations(0, oldReplicas, newReplicas, callback);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5702), uuids[1]), 1, -1, new PartitionReplica(new Address("localhost", 5704), uuids[3]), 3, 1);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5701), uuids[0]), 0, -1, new PartitionReplica(new Address("localhost", 5702), uuids[1]), -1, 0);
    }

    @Test
    public void test_SHIFT_UP_multipleTimes() throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                null,
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                null,
                null,
                null,
        };
        final PartitionReplica[] newReplicas = {
                new PartitionReplica(new Address("localhost", 5702), uuids[1]),
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]),
                null,
                null,
                null,
                null,
        };

        migrationPlanner.planMigrations(0, oldReplicas, newReplicas, callback);
        verify(callback).migrate(null, -1, -1, new PartitionReplica(new Address("localhost", 5703), uuids[2]), 2, 1);
        verify(callback).migrate(null, -1, -1, new PartitionReplica(new Address("localhost", 5704), uuids[3]), 3, 2);
    }

    @Test
    public void test_SHIFT_UP_nonNullSource_isNoLongerReplica() throws UnknownHostException {
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
                null,
                null,
                null,
                null,
                null,
                null,
        };

        migrationPlanner.planMigrations(0, oldReplicas, newReplicas, callback);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5701), uuids[0]), 0, -1, new PartitionReplica(new Address("localhost", 5702), uuids[1]), 1, 0);
    }

    @Test
    public void test_SHIFT_UP_nonNullSource_willGetAnotherMOVE() throws UnknownHostException {
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
                null,
                null,
                null,
                null,
                null,
        };

        migrationPlanner.planMigrations(0, oldReplicas, newReplicas, callback);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5701), uuids[0]), 0, -1, new PartitionReplica(new Address("localhost", 5703), uuids[2]), 2, 0);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5702), uuids[1]), 1, -1, new PartitionReplica(new Address("localhost", 5701), uuids[0]), -1, 1);
    }

    @Test
    public void test_SHIFT_UP_SHIFT_DOWN_atomicTogether() throws UnknownHostException {
        final PartitionReplica[] oldReplicas = {
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                null,
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                null,
                null,
                null,
                null,
        };
        final PartitionReplica[] newReplicas = {
                new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                null,
                null,
                null,
                null,
                null,
        };

        migrationPlanner.planMigrations(0, oldReplicas, newReplicas, callback);
        verify(callback).migrate(new PartitionReplica(new Address("localhost", 5701), uuids[0]), 0, 1, new PartitionReplica(new Address("localhost", 5703), uuids[2]), 2, 0);
    }

    @Test
    public void testSingleMigrationPrioritization() throws UnknownHostException {
        List<MigrationInfo> migrations = new ArrayList<MigrationInfo>();
        final MigrationInfo migration1 = new MigrationInfo(0, null, new PartitionReplica(new Address("localhost", 5701), uuids[0]), -1, -1, -1, 0);
        migrations.add(migration1);

        migrationPlanner.prioritizeCopiesAndShiftUps(migrations);

        assertEquals(singletonList(migration1), migrations);
    }

    @Test
    public void testNoCopyPrioritizationAgainstCopy() throws UnknownHostException {
        List<MigrationInfo> migrations = new ArrayList<MigrationInfo>();
        final MigrationInfo migration1 = new MigrationInfo(0, null, new PartitionReplica(new Address("localhost", 5701), uuids[0]), -1, -1, -1, 0);
        final MigrationInfo migration2 = new MigrationInfo(0, null, new PartitionReplica(new Address("localhost", 5702), uuids[1]), -1, -1, -1, 1);
        final MigrationInfo migration3 = new MigrationInfo(0, null, new PartitionReplica(new Address("localhost", 5703), uuids[2]), -1, -1, -1, 2);
        final MigrationInfo migration4 = new MigrationInfo(0, null, new PartitionReplica(new Address("localhost", 5704), uuids[3]), -1, -1, -1, 3);
        migrations.add(migration1);
        migrations.add(migration2);
        migrations.add(migration3);
        migrations.add(migration4);

        migrationPlanner.prioritizeCopiesAndShiftUps(migrations);

        assertEquals(asList(migration1, migration2, migration3, migration4), migrations);
    }

    @Test
    public void testCopyPrioritizationAgainstMove()
            throws UnknownHostException {
        List<MigrationInfo> migrations = new ArrayList<MigrationInfo>();
        final MigrationInfo migration1 = new MigrationInfo(0, null, new PartitionReplica(new Address("localhost", 5701), uuids[0]), -1, -1, -1, 0);
        final MigrationInfo migration2 = new MigrationInfo(0, null, new PartitionReplica(new Address("localhost", 5702), uuids[1]), -1, -1, -1, 1);
        final MigrationInfo migration3 = new MigrationInfo(0, new PartitionReplica(new Address("localhost", 5703), uuids[2]),
                new PartitionReplica(new Address("localhost", 5704), uuids[3]), 2, -1, -1, 2);
        final MigrationInfo migration4 = new MigrationInfo(0, new PartitionReplica(new Address("localhost", 5705), uuids[4]),
                new PartitionReplica(new Address("localhost", 5706), uuids[5]), 2, -1, -1, 3);
        final MigrationInfo migration5 = new MigrationInfo(0, null, new PartitionReplica(new Address("localhost", 5707), uuids[6]), -1, -1, -1, 4);
        migrations.add(migration1);
        migrations.add(migration2);
        migrations.add(migration3);
        migrations.add(migration4);
        migrations.add(migration5);

        migrationPlanner.prioritizeCopiesAndShiftUps(migrations);

        assertEquals(asList(migration1, migration2, migration5, migration3, migration4), migrations);
    }

    @Test
    public void testShiftUpPrioritizationAgainstMove() throws UnknownHostException {
        List<MigrationInfo> migrations = new ArrayList<MigrationInfo>();
        final MigrationInfo migration1 = new MigrationInfo(0, null, new PartitionReplica(new Address("localhost", 5701), uuids[0]), -1, -1, -1, 0);
        final MigrationInfo migration2 = new MigrationInfo(0, null, new PartitionReplica(new Address("localhost", 5702), uuids[1]), -1, -1, -1, 1);
        final MigrationInfo migration3 = new MigrationInfo(0, new PartitionReplica(new Address("localhost", 5705), uuids[4]),
                new PartitionReplica(new Address("localhost", 5706), uuids[5]), 2, -1, -1, 3);
        final MigrationInfo migration4 = new MigrationInfo(0, null, new PartitionReplica(new Address("localhost", 5707), uuids[6]), -1, -1, 4, 2);
        migrations.add(migration1);
        migrations.add(migration2);
        migrations.add(migration3);
        migrations.add(migration4);

        migrationPlanner.prioritizeCopiesAndShiftUps(migrations);

        assertEquals(asList(migration1, migration2, migration4, migration3), migrations);
    }

    @Test
    public void testCopyPrioritizationAgainstShiftDownToColderIndex() throws UnknownHostException {
        List<MigrationInfo> migrations = new ArrayList<MigrationInfo>();
        final MigrationInfo migration1 = new MigrationInfo(0, new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]), 0, 2, -1, 0);
        final MigrationInfo migration2 = new MigrationInfo(0, null, new PartitionReplica(new Address("localhost", 5703), uuids[2]), -1, -1, -1, 1);

        migrations.add(migration1);
        migrations.add(migration2);

        migrationPlanner.prioritizeCopiesAndShiftUps(migrations);

        assertEquals(asList(migration2, migration1), migrations);
    }

    @Test
    public void testNoCopyPrioritizationAgainstShiftDownToHotterIndex() throws UnknownHostException {
        List<MigrationInfo> migrations = new ArrayList<MigrationInfo>();
        final MigrationInfo migration1 = new MigrationInfo(0, new PartitionReplica(new Address("localhost", 5701), uuids[0]),
                new PartitionReplica(new Address("localhost", 5702), uuids[1]), 0, 1, -1, 0);
        final MigrationInfo migration2 = new MigrationInfo(0, null, new PartitionReplica(new Address("localhost", 5703), uuids[2]), -1, -1, -1, 2);

        migrations.add(migration1);
        migrations.add(migration2);

        migrationPlanner.prioritizeCopiesAndShiftUps(migrations);

        assertEquals(asList(migration1, migration2), migrations);
    }

    @Test
    public void testRandom() throws UnknownHostException {
        for (int i = 0; i < 100; i++) {
            testRandom(3);
            testRandom(4);
            testRandom(5);
        }
    }

    private void testRandom(int initialLen) throws UnknownHostException {
        PartitionReplica[] oldReplicas = new PartitionReplica[InternalPartition.MAX_REPLICA_COUNT];
        for (int i = 0; i < initialLen; i++) {
            oldReplicas[i] = new PartitionReplica(newAddress(5000 + i), UuidUtil.newUnsecureUUID());
        }

        PartitionReplica[] newReplicas = Arrays.copyOf(oldReplicas, oldReplicas.length);
        int newLen = (int) (Math.random() * (oldReplicas.length - initialLen + 1));
        for (int i = 0; i < newLen; i++) {
            newReplicas[i + initialLen] = new PartitionReplica(newAddress(6000 + i), UuidUtil.newUnsecureUUID());
        }

        shuffle(newReplicas, initialLen + newLen);

        migrationPlanner.planMigrations(0, oldReplicas, newReplicas, callback);
    }

    private void shuffle(PartitionReplica[] array, int len) {
        int index;
        PartitionReplica temp;
        Random random = new Random();
        for (int i = len - 1; i > 0; i--) {
            index = random.nextInt(i + 1);
            temp = array[index];
            array[index] = array[i];
            array[i] = temp;
        }
    }

    private Address newAddress(int port)
            throws UnknownHostException {
        return new Address("localhost", port);
    }
}
