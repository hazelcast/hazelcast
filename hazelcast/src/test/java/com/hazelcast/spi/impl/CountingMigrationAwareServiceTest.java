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

package com.hazelcast.spi.impl;

import com.hazelcast.internal.partition.FragmentedMigrationAwareService;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.spi.impl.CountingMigrationAwareService.IN_FLIGHT_MIGRATION_STAMP;
import static com.hazelcast.spi.impl.CountingMigrationAwareService.PRIMARY_REPLICA_INDEX;
import static com.hazelcast.spi.impl.CountingMigrationAwareService.isPrimaryReplicaMigrationEvent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test count-tracking functionality of CountingMigrationAwareService
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CountingMigrationAwareServiceTest {

    @Parameterized.Parameter
    public FragmentedMigrationAwareService wrappedMigrationAwareService;

    @Parameterized.Parameter(1)
    public PartitionMigrationEvent event;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private CountingMigrationAwareService countingMigrationAwareService;
    private int initialMigrationStamp;

    @Parameterized.Parameters(name = "{0}, replica: {1}")
    public static Collection<Object> parameters() {
        PartitionMigrationEvent promotionEvent = mock(PartitionMigrationEvent.class);
        when(promotionEvent.getNewReplicaIndex()).thenReturn(PRIMARY_REPLICA_INDEX);
        when(promotionEvent.getCurrentReplicaIndex()).thenReturn(1);
        when(promotionEvent.toString()).thenReturn("1 > 0");
        PartitionMigrationEvent demotionEvent = mock(PartitionMigrationEvent.class);
        when(demotionEvent.getNewReplicaIndex()).thenReturn(1);
        when(demotionEvent.getCurrentReplicaIndex()).thenReturn(PRIMARY_REPLICA_INDEX);
        when(demotionEvent.toString()).thenReturn("0 > 1");
        PartitionMigrationEvent backupsEvent = mock(PartitionMigrationEvent.class);
        when(backupsEvent.getNewReplicaIndex()).thenReturn(1);
        when(backupsEvent.getCurrentReplicaIndex()).thenReturn(2);
        when(backupsEvent.toString()).thenReturn("2 > 1");

        return Arrays.asList(new Object[]{
                new Object[]{new NoOpMigrationAwareService(), promotionEvent},
                new Object[]{new NoOpMigrationAwareService(), demotionEvent},
                new Object[]{new NoOpMigrationAwareService(), backupsEvent},
                new Object[]{new ExceptionThrowingMigrationAwareService(), promotionEvent},
                new Object[]{new ExceptionThrowingMigrationAwareService(), demotionEvent},
                new Object[]{new ExceptionThrowingMigrationAwareService(), backupsEvent},
        });
    }

    @Before
    public void setUp() throws Exception {
        // setup the counting migration aware service and execute 1 prepareReplicationOperation (which does not
        // affect the counter)
        countingMigrationAwareService = new CountingMigrationAwareService(wrappedMigrationAwareService);
        initialMigrationStamp = countingMigrationAwareService.getMigrationStamp();
        countingMigrationAwareService.prepareReplicationOperation(null);
        // also execute the first part of migration: beforeMigration
        try {
            countingMigrationAwareService.beforeMigration(event);
        } catch (RuntimeException e) {
            // we do not care whether the wrapped service throws an exception
        }
    }

    @Test
    public void beforeMigration() throws Exception {
        // when: countingMigrationAwareService.beforeMigration was invoked (in setUp method)
        // then: if event involves primary replica, stamp should change.
        if (isPrimaryReplicaMigrationEvent(event)) {
            assertEquals(IN_FLIGHT_MIGRATION_STAMP, countingMigrationAwareService.getMigrationStamp());
            assertFalse(countingMigrationAwareService.validateMigrationStamp(IN_FLIGHT_MIGRATION_STAMP));
        } else {
            assertEquals(initialMigrationStamp, countingMigrationAwareService.getMigrationStamp());
            assertTrue(countingMigrationAwareService.validateMigrationStamp(initialMigrationStamp));
        }
    }

    @Test
    public void commitMigration() throws Exception {
        // when: before - commit migration methods have been executed
        try {
            countingMigrationAwareService.commitMigration(event);
        } catch (RuntimeException e) {
            // we do not care whether the wrapped service throws an exception
        }

        int currentMigrationStamp = countingMigrationAwareService.getMigrationStamp();
        // then: if event involves primary replica, stamp should change.
        if (isPrimaryReplicaMigrationEvent(event)) {
            assertNotEquals(initialMigrationStamp, currentMigrationStamp);
        } else {
            assertEquals(initialMigrationStamp, currentMigrationStamp);
        }
        assertTrue(countingMigrationAwareService.validateMigrationStamp(currentMigrationStamp));
    }

    @Test
    public void rollbackMigration() throws Exception {
        // when: before - rollback migration methods have been executed
        try {
            countingMigrationAwareService.rollbackMigration(event);
        } catch (RuntimeException e) {
            // we do not care whether the wrapped service throws an exception
        }

        int currentMigrationStamp = countingMigrationAwareService.getMigrationStamp();
        // then: if event involves primary replica, stamp should change.
        if (isPrimaryReplicaMigrationEvent(event)) {
            assertNotEquals(initialMigrationStamp, currentMigrationStamp);
        } else {
            assertEquals(initialMigrationStamp, currentMigrationStamp);
        }
        assertTrue(countingMigrationAwareService.validateMigrationStamp(currentMigrationStamp));
    }

    @Test
    public void commitMigration_invalidCount_throwsAssertionError() {
        // when: invalid sequence of beforeMigration, commitMigration, commitMigration is executed
        // and
        try {
            countingMigrationAwareService.commitMigration(event);
        } catch (RuntimeException e) {
            // we do not care whether the wrapped service throws an exception
        }

        // on second commitMigration, if event involves partition owner assertion error is thrown
        if (isPrimaryReplicaMigrationEvent(event)) {
            expectedException.expect(AssertionError.class);
        }
        try {
            countingMigrationAwareService.commitMigration(event);
        } catch (RuntimeException e) {
            // we do not care whether the wrapped service throws an exception
        }
    }

    @Test
    public void rollbackMigration_invalidCount_throwsAssertionError() {
        // when: invalid sequence of beforeMigration, rollbackMigration, rollbackMigration is executed
        try {
            countingMigrationAwareService.rollbackMigration(event);
        } catch (RuntimeException e) {
            // we do not care whether the wrapped service throws an exception
        }

        // on second rollbackMigration, if event involves partition owner assertion error is thrown
        if (isPrimaryReplicaMigrationEvent(event)) {
            expectedException.expect(AssertionError.class);
        }
        try {
            countingMigrationAwareService.rollbackMigration(event);
        } catch (RuntimeException e) {
            // we do not care whether the wrapped service throws an exception
        }
    }

    static class ExceptionThrowingMigrationAwareService implements FragmentedMigrationAwareService {
        @Override
        public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
            return null;
        }

        @Override
        public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
            return false;
        }

        @Override
        public Operation prepareReplicationOperation(PartitionReplicationEvent event,
                                                     Collection<ServiceNamespace> namespaces) {
            return null;
        }

        @Override
        public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
            return null;
        }

        @Override
        public void beforeMigration(PartitionMigrationEvent event) {
            throw new RuntimeException("");
        }

        @Override
        public void commitMigration(PartitionMigrationEvent event) {
            throw new RuntimeException("");
        }

        @Override
        public void rollbackMigration(PartitionMigrationEvent event) {
            throw new RuntimeException("");
        }

        @Override
        public String toString() {
            return "ExceptionThrowingMigrationAwareService";
        }
    }


    static class NoOpMigrationAwareService implements FragmentedMigrationAwareService {
        @Override
        public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
            return null;
        }

        @Override
        public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
            return false;
        }

        @Override
        public Operation prepareReplicationOperation(PartitionReplicationEvent event,
                                                     Collection<ServiceNamespace> namespaces) {
            return null;
        }

        @Override
        public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
            return null;
        }

        @Override
        public void beforeMigration(PartitionMigrationEvent event) {

        }

        @Override
        public void commitMigration(PartitionMigrationEvent event) {

        }

        @Override
        public void rollbackMigration(PartitionMigrationEvent event) {

        }

        @Override
        public String toString() {
            return "NoOpMigrationAwareService";
        }
    }
}
