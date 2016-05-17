package com.hazelcast.internal.partition.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalMigrationListener.MigrationParticipant;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InternalMigrationListenerTest
        extends HazelcastTestSupport {

    private static final int PARTITION_COUNT = 2;

    @Test
    public void shouldInvokeInternalMigrationListenerOnSuccessfulMigration() {
        final Config config1 = new Config();
        config1.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);
        warmUpPartitions(hz1);

        final InternalMigrationListenerImpl listener = new InternalMigrationListenerImpl();
        final Config config2 = new Config();
        config2.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));
        config2.addListenerConfig(new ListenerConfig(listener));
        final HazelcastInstance hz2 = factory.newHazelcastInstance(config2);

        waitAllForSafeState(hz1, hz2);

        final List<Integer> hz2PartitionIds = getNodeEngineImpl(hz2).getPartitionService().getMemberPartitions(getAddress(hz2));
        assertEquals(1, hz2PartitionIds.size());
        final int hz2PartitionId = hz2PartitionIds.get(0);

        final List<MigrationProgressNotification> notifications = listener.getNotifications();

        int partition0Events = 0, partition1Events = 0;
        assertEquals(6, notifications.size());

        for (MigrationProgressNotification n : notifications) {
            if ( n.migrationInfo.getPartitionId() == 0) {
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
