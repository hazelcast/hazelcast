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

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalMigrationListener.MigrationParticipant;
import com.hazelcast.internal.partition.impl.MigrationCommitTest.DelayMigrationStart;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class InternalMigrationListenerTest extends HazelcastTestSupport {

    private static final int PARTITION_COUNT = 2;

    @Test
    public void shouldInvokeInternalMigrationListenerOnSuccessfulMigration() {
        final Config config1 = new Config();
        config1.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));

        // hold the migrations until all nodes join so that there will be no retries / failed migrations etc.
        final CountDownLatch migrationStartLatch = new CountDownLatch(1);
        config1.addListenerConfig(new ListenerConfig(new DelayMigrationStart(migrationStartLatch)));

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);
        warmUpPartitions(hz1);

        final InternalMigrationListenerImpl listener = new InternalMigrationListenerImpl();
        final Config config2 = new Config();
        config2.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));
        config2.addListenerConfig(new ListenerConfig(listener));
        final HazelcastInstance hz2 = factory.newHazelcastInstance(config2);

        migrationStartLatch.countDown();

        waitAllForSafeState(hz1, hz2);

        final List<Integer> hz2PartitionIds = getNodeEngineImpl(hz2).getPartitionService().getMemberPartitions(getAddress(hz2));
        assertEquals(1, hz2PartitionIds.size());

        final List<MigrationProgressNotification> notifications = listener.getNotifications();

        int partition0Events = 0, partition1Events = 0;
        // 3 event for migration = START -> COMPLETE -> COMMIT/ROLLBACK
        assertEquals("Migration Notifications: " + notifications, PARTITION_COUNT * 3, notifications.size());

        for (MigrationProgressNotification n : notifications) {
            if (n.migrationInfo.getPartitionId() == 0) {
                partition0Events++;
            } else {
                partition1Events++;
            }
        }

        assertEquals(3, partition0Events);
        assertEquals(3, partition1Events);
    }

    enum MigrationProgressEvent {
        START,
        COMPLETE,
        COMMIT,
        ROLLBACK
    }

    static class MigrationProgressNotification {

        final MigrationProgressEvent event;

        final MigrationParticipant participant;

        final MigrationInfo migrationInfo;

        final boolean success;

        public MigrationProgressNotification(MigrationProgressEvent event, MigrationParticipant participant,
                                             MigrationInfo migrationInfo) {
            this(event, participant, migrationInfo, true);
        }

        public MigrationProgressNotification(MigrationProgressEvent event, MigrationParticipant participant,
                                             MigrationInfo migrationInfo, boolean success) {
            this.event = event;
            this.participant = participant;
            this.migrationInfo = migrationInfo;
            this.success = success;
        }

        @Override
        public String toString() {
            return "MigrationProgressNotification{" +
                    "event=" + event +
                    ", participant=" + participant +
                    ", migrationInfo=" + migrationInfo +
                    ", success=" + success +
                    '}';
        }
    }

    static class InternalMigrationListenerImpl
            extends InternalMigrationListener {

        private final List<MigrationProgressNotification> notifications = new ArrayList<MigrationProgressNotification>();

        @Override
        public synchronized void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
            notifications.add(new MigrationProgressNotification(MigrationProgressEvent.START, participant, migrationInfo));
        }

        @Override
        public synchronized void onMigrationComplete(MigrationParticipant participant, MigrationInfo migrationInfo,
                                                     boolean success) {
            notifications
                    .add(new MigrationProgressNotification(MigrationProgressEvent.COMPLETE, participant, migrationInfo, success));
        }

        @Override
        public synchronized void onMigrationCommit(MigrationParticipant participant, MigrationInfo migrationInfo) {
            notifications.add(new MigrationProgressNotification(MigrationProgressEvent.COMMIT, participant, migrationInfo));
        }

        @Override
        public synchronized void onMigrationRollback(MigrationParticipant participant, MigrationInfo migrationInfo) {
            notifications.add(new MigrationProgressNotification(MigrationProgressEvent.ROLLBACK, participant, migrationInfo));
        }

        public synchronized List<MigrationProgressNotification> getNotifications() {
            return new ArrayList<MigrationProgressNotification>(notifications);
        }

    }

}
