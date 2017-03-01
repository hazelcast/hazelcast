/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.MigrationPlanner.MigrationDecisionCallback;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MigrationPlannerTest {

    private MigrationDecisionCallback callback = mock(MigrationDecisionCallback.class);
    private MigrationPlanner migrationPlanner = new MigrationPlanner();

    @Test
    public void test_MOVE()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), null, null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5704), new Address("localhost", 5702), new Address(
                "localhost", 5705), null, null, null, null};

        migrationPlanner.planMigrations(oldAddresses, newAddresses, callback);
        verify(callback).migrate(new Address("localhost", 5701), 0, -1, new Address("localhost", 5704), -1, 0);
        verify(callback).migrate(new Address("localhost", 5703), 2, -1, new Address("localhost", 5705), -1, 2);
    }

    @Test
    public void test_COPY()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), null, new Address("localhost",
                5703), null, null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5704), new Address(
                "localhost", 5703), null, null, null, null};

        migrationPlanner.planMigrations(oldAddresses, newAddresses, callback);
        verify(callback).migrate(null, -1, -1, new Address("localhost", 5704), -1, 1);
    }

    @Test
    public void test_SHIFT_DOWN_withNullKeepReplicaIndex()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), null, new Address("localhost",
                5703), null, null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5704), new Address("localhost", 5701), new Address(
                "localhost", 5703), null, null, null, null};

        migrationPlanner.planMigrations(oldAddresses, newAddresses, callback);
        verify(callback).migrate(new Address("localhost", 5701), 0, 1, new Address("localhost", 5704), -1, 0);
    }

    @Test
    public void test_SHIFT_DOWN_withNullNonNullKeepReplicaIndex()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), null, null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5704), new Address("localhost", 5701), new Address(
                "localhost", 5703), null, null, null, null};

        migrationPlanner.planMigrations(oldAddresses, newAddresses, callback);
        verify(callback).migrate(new Address("localhost", 5701), 0, -1, new Address("localhost", 5704), -1, 0);
        verify(callback).migrate(new Address("localhost", 5702), 1, -1, new Address("localhost", 5701), -1, 1);
    }

    @Test
    public void test_SHIFT_DOWN_performedBy_MOVE()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), null, null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5704), new Address("localhost", 5701), new Address(
                "localhost", 5702), null, null, null, null};

        migrationPlanner.planMigrations(oldAddresses, newAddresses, callback);
        verify(callback).migrate(new Address("localhost", 5701), 0, -1, new Address("localhost", 5704), -1, 0);
        verify(callback).migrate(new Address("localhost", 5702), 1, -1, new Address("localhost", 5701), -1, 1);
        verify(callback).migrate(new Address("localhost", 5703), 2, -1, new Address("localhost", 5702), -1, 2);
    }

    @Test
    public void test_SHIFT_UP()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), null, new Address("localhost",
                5703), new Address("localhost", 5704), null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5703), new Address(
                "localhost", 5704), null, null, null, null};

        migrationPlanner.planMigrations(oldAddresses, newAddresses, callback);

        verify(callback).migrate(null, -1, -1, new Address("localhost", 5703), 2, 1);
        verify(callback).migrate(null, -1, -1, new Address("localhost", 5704), 3, 2);
    }

    @Test
    public void test_SHIFT_UPS_performedBy_MOVE()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), new Address("localhost", 5704), null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5703), new Address(
                "localhost", 5704), new Address("localhost", 5705), null, null, null};

        migrationPlanner.planMigrations(oldAddresses, newAddresses, callback);

        verify(callback).migrate(new Address("localhost", 5704), 3, -1, new Address("localhost", 5705), -1, 3);
        verify(callback).migrate(new Address("localhost", 5703), 2, -1, new Address("localhost", 5704), -1, 2);
        verify(callback).migrate(new Address("localhost", 5702), 1, -1, new Address("localhost", 5703), -1, 1);
    }

    @Test
    public void test_SHIFT_DOWN_performedAfterKnownNewReplicaOwnerKickedOutOfReplicas()
            throws UnknownHostException {

        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), new Address("localhost", 5705), null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5704), new Address("localhost", 5703), new Address(
                "localhost", 5705), new Address("localhost", 5706), new Address("localhost", 5702), new Address("localhost",
                5701), null};

        migrationPlanner.planMigrations(oldAddresses, newAddresses, callback);
        verify(callback).migrate(new Address("localhost", 5701), 0, 5, new Address("localhost", 5704), -1, 0);
        verify(callback).migrate(new Address("localhost", 5705), 3, -1, new Address("localhost", 5706), -1, 3);
        verify(callback).migrate(new Address("localhost", 5703), 2, -1, new Address("localhost", 5705), -1, 2);
        verify(callback).migrate(new Address("localhost", 5702), 1, 4, new Address("localhost", 5703), -1, 1);
    }

    @Test
    public void test_SHIFT_DOWN_performedBeforeNonConflicting_SHIFT_UP()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), new Address("localhost", 5705), null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5704), new Address("localhost", 5703), new Address(
                "localhost", 5705), new Address("localhost", 5706), new Address("localhost", 5701), null, null};

        migrationPlanner.planMigrations(oldAddresses, newAddresses, callback);
        verify(callback).migrate(new Address("localhost", 5701), 0, 4, new Address("localhost", 5704), -1, 0);
        verify(callback).migrate(new Address("localhost", 5705), 3, -1, new Address("localhost", 5706), -1, 3);
        verify(callback).migrate(new Address("localhost", 5703), 2, -1, new Address("localhost", 5705), -1, 2);
    }

    @Test
    public void test_MOVE_toNull()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), new Address("localhost", 5705), null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), null, null, null, null};

        migrationPlanner.planMigrations(oldAddresses, newAddresses, callback);
        verify(callback).migrate(new Address("localhost", 5705), 3, -1, null, -1, -1);
    }

    @Test
    public void test_SHIFT_UP_toReplicaIndexWithExistingOwner()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), new Address("localhost", 5704), null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5704), new Address(
                "localhost", 5703), null, null, null, null};

        migrationPlanner.planMigrations(oldAddresses, newAddresses, callback);
        verify(callback).migrate(new Address("localhost", 5702), 1, -1, new Address("localhost", 5704), 3, 1);
    }

    @Test
    public void test_MOVE_performedAfter_SHIFT_UP_toReplicaIndexWithExistingOwnerKicksItOutOfCluster()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost", 5702), new Address(
                "localhost", 5703), new Address("localhost", 5704), null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5702), new Address("localhost", 5704), new Address(
                "localhost", 5703), null, null, null, null};

        migrationPlanner.planMigrations(oldAddresses, newAddresses, callback);
        verify(callback).migrate(new Address("localhost", 5702), 1, -1, new Address("localhost", 5704), 3, 1);
        verify(callback).migrate(new Address("localhost", 5701), 0, -1, new Address("localhost", 5702), -1, 0);
    }

    @Test
    public void test_SHIFT_UP_multipleTimes()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5702), null, new Address("localhost",
                5703), new Address("localhost", 5704), null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5702), new Address("localhost", 5703), new Address(
                "localhost", 5704), null, null, null, null};

        migrationPlanner.planMigrations(oldAddresses, newAddresses, callback);
        verify(callback).migrate(null, -1, -1, new Address("localhost", 5703), 2, 1);
        verify(callback).migrate(null, -1, -1, new Address("localhost", 5704), 3, 2);
    }

    @Test
    public void test_SHIFT_UP_nonNullSource_isNoLongerReplica()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost",
                5702), null, null, null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5702), null, null, null, null, null, null};

        migrationPlanner.planMigrations(oldAddresses, newAddresses, callback);
        verify(callback).migrate(new Address("localhost", 5701), 0, -1, new Address("localhost", 5702), 1, 0);
    }

    @Test
    public void test_SHIFT_UP_nonNullSource_willGetAnotherMOVE()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), new Address("localhost",
                5702), new Address("localhost", 5703), null, null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5703), new Address("localhost", 5701), null, null, null, null, null};

        migrationPlanner.planMigrations(oldAddresses, newAddresses, callback);
        verify(callback).migrate(new Address("localhost", 5701), 0, -1, new Address("localhost", 5703), 2, 0);
        verify(callback).migrate(new Address("localhost", 5702), 1, -1, new Address("localhost", 5701), -1, 1);
    }

    @Test
    public void test_SHIFT_UP_SHIFT_DOWN_atomicTogether()
            throws UnknownHostException {
        final Address[] oldAddresses = new Address[]{new Address("localhost", 5701), null, new Address("localhost",
                5703), null, null, null, null};

        final Address[] newAddresses = new Address[]{new Address("localhost", 5703), new Address("localhost", 5701), null, null, null, null, null};

        migrationPlanner.planMigrations(oldAddresses, newAddresses, callback);
        verify(callback).migrate(new Address("localhost", 5701), 0, 1, new Address("localhost", 5703), 2, 0);
    }

    @Test
    public void testSingleMigrationPrioritization()
            throws UnknownHostException {
        List<MigrationInfo> migrations = new ArrayList<MigrationInfo>();
        final MigrationInfo migration1 = new MigrationInfo(0, null, null, new Address("localhost", 5701), "5701", -1, -1, -1, 0);
        migrations.add(migration1);

        migrationPlanner.prioritizeCopiesAndShiftUps(migrations);

        assertEquals(singletonList(migration1), migrations);
    }

    @Test
    public void testNoCopyPrioritizationAgainstCopy()
            throws UnknownHostException {
        List<MigrationInfo> migrations = new ArrayList<MigrationInfo>();
        final MigrationInfo migration1 = new MigrationInfo(0, null, null, new Address("localhost", 5701), "5701", -1, -1, -1, 0);
        final MigrationInfo migration2 = new MigrationInfo(0, null, null, new Address("localhost", 5702), "5702", -1, -1, -1, 1);
        final MigrationInfo migration3 = new MigrationInfo(0, null, null, new Address("localhost", 5703), "5702", -1, -1, -1, 2);
        final MigrationInfo migration4 = new MigrationInfo(0, null, null, new Address("localhost", 5704), "5702", -1, -1, -1, 3);
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
        final MigrationInfo migration1 = new MigrationInfo(0, null, null, new Address("localhost", 5701), "5701", -1, -1, -1, 0);
        final MigrationInfo migration2 = new MigrationInfo(0, null, null, new Address("localhost", 5702), "5702", -1, -1, -1, 1);
        final MigrationInfo migration3 = new MigrationInfo(0, new Address("localhost", 5703), "5703",
                new Address("localhost", 5704), "5704", 2, -1, -1, 2);
        final MigrationInfo migration4 = new MigrationInfo(0, new Address("localhost", 5705), "5705",
                new Address("localhost", 5706), "5706", 2, -1, -1, 3);
        final MigrationInfo migration5 = new MigrationInfo(0, null, null, new Address("localhost", 5707), "5707", -1, -1, -1, 4);
        migrations.add(migration1);
        migrations.add(migration2);
        migrations.add(migration3);
        migrations.add(migration4);
        migrations.add(migration5);

        migrationPlanner.prioritizeCopiesAndShiftUps(migrations);

        assertEquals(asList(migration1, migration2, migration5, migration3, migration4), migrations);
    }

    @Test
    public void testShiftUpPrioritizationAgainstMove()
            throws UnknownHostException {
        List<MigrationInfo> migrations = new ArrayList<MigrationInfo>();
        final MigrationInfo migration1 = new MigrationInfo(0, null, null, new Address("localhost", 5701), "5701", -1, -1, -1, 0);
        final MigrationInfo migration2 = new MigrationInfo(0, null, null, new Address("localhost", 5702), "5702", -1, -1, -1, 1);
        final MigrationInfo migration3 = new MigrationInfo(0, new Address("localhost", 5705), "5705",
                new Address("localhost", 5706), "5706", 2, -1, -1, 3);
        final MigrationInfo migration4 = new MigrationInfo(0, null, null, new Address("localhost", 5707), "5707", -1, -1, 4, 2);
        migrations.add(migration1);
        migrations.add(migration2);
        migrations.add(migration3);
        migrations.add(migration4);

        migrationPlanner.prioritizeCopiesAndShiftUps(migrations);

        assertEquals(asList(migration1, migration2, migration4, migration3), migrations);
    }

    @Test
    public void testCopyPrioritizationAgainstShiftDownToColderIndex()
            throws UnknownHostException {
        List<MigrationInfo> migrations = new ArrayList<MigrationInfo>();
        final MigrationInfo migration1 = new MigrationInfo(0, new Address("localhost", 5701), "5701", new Address("localhost", 5702), "5702", 0, 2, -1, 0);
        final MigrationInfo migration2 = new MigrationInfo(0, null, null, new Address("localhost", 5703), "5703", -1, -1, -1, 1);

        migrations.add(migration1);
        migrations.add(migration2);

        migrationPlanner.prioritizeCopiesAndShiftUps(migrations);

        assertEquals(asList(migration2, migration1), migrations);
    }

    @Test
    public void testNoCopyPrioritizationAgainstShiftDownToHotterIndex()
            throws UnknownHostException {
        List<MigrationInfo> migrations = new ArrayList<MigrationInfo>();
        final MigrationInfo migration1 = new MigrationInfo(0, new Address("localhost", 5701), "5701", new Address("localhost", 5702), "5702", 0, 1, -1, 0);
        final MigrationInfo migration2 = new MigrationInfo(0, null, null, new Address("localhost", 5703), "5703", -1, -1, -1, 2);

        migrations.add(migration1);
        migrations.add(migration2);

        migrationPlanner.prioritizeCopiesAndShiftUps(migrations);

        assertEquals(asList(migration1, migration2), migrations);
    }

    @Test
    public void testRandom()
            throws UnknownHostException {
        for (int i = 0; i < 100; i++) {
            testRandom(3);
            testRandom(4);
            testRandom(5);
        }
    }

    private void testRandom(int initialLen)
            throws java.net.UnknownHostException {
        Address[] oldAddresses = new Address[InternalPartition.MAX_REPLICA_COUNT];
        for (int i = 0; i < initialLen; i++) {
            oldAddresses[i] = newAddress(5000 + i);
        }

        Address[] newAddresses = Arrays.copyOf(oldAddresses, oldAddresses.length);
        int newLen = (int) (Math.random() * (oldAddresses.length - initialLen + 1));
        for (int i = 0; i < newLen; i++) {
            newAddresses[i + initialLen] = newAddress(6000 + i);
        }

        shuffle(newAddresses, initialLen + newLen);

        migrationPlanner.planMigrations(oldAddresses, newAddresses, callback);
    }

    private void shuffle(Address[] array, int len) {
        int index;
        Address temp;
        Random random = new Random();
        for (int i = len - 1; i > 0; i--) {
            index = random.nextInt(i + 1);
            temp = array[index];
            array[index] = array[i];
            array[i] = temp;
        }
    }

    private Address newAddress(int port)
            throws java.net.UnknownHostException {
        return new Address("localhost", port);
    }

}
