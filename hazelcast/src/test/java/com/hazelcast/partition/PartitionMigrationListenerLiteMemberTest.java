package com.hazelcast.partition;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PartitionMigrationListenerLiteMemberTest
        extends HazelcastTestSupport {

    @Test
    public void testMigrationListenerOnLiteMember()
            throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        factory.newHazelcastInstance();

        final Config liteConfig = new Config().setLiteMember(true);
        final HazelcastInstance lite = factory.newHazelcastInstance(liteConfig);

        warmUpPartitions(lite);
        final DummyMigrationListener listener = new DummyMigrationListener();
        lite.getPartitionService().addMigrationListener(listener);

        factory.newHazelcastInstance();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(listener.started.get());
                assertTrue(listener.completed.get());
            }
        });
    }

    private static class DummyMigrationListener
            implements MigrationListener {

        private AtomicBoolean started = new AtomicBoolean();

        private AtomicBoolean completed = new AtomicBoolean();

        @Override
        public void migrationStarted(MigrationEvent migrationEvent) {
            started.compareAndSet(false, true);
        }

        @Override
        public void migrationCompleted(MigrationEvent migrationEvent) {
            completed.compareAndSet(false, true);
        }

        @Override
        public void migrationFailed(MigrationEvent migrationEvent) {

        }

    }
}
