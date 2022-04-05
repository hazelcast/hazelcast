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

package com.hazelcast.partition;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionMigrationListenerLiteMemberTest extends HazelcastTestSupport {

    @Test
    public void testMigrationListenerOnLiteMember() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        factory.newHazelcastInstance();

        Config liteConfig = new Config().setLiteMember(true);
        HazelcastInstance lite = factory.newHazelcastInstance(liteConfig);

        warmUpPartitions(lite);
        final DummyMigrationListener listener = new DummyMigrationListener();
        lite.getPartitionService().addMigrationListener(listener);

        factory.newHazelcastInstance();

        assertTrueEventually(() -> {
            assertTrue(listener.started.get());
            assertTrue(listener.completed.get());
        });
    }

    private static class DummyMigrationListener implements MigrationListener {

        private final AtomicBoolean started = new AtomicBoolean();
        private final AtomicBoolean completed = new AtomicBoolean();

        @Override
        public void migrationStarted(MigrationState state) {
            started.compareAndSet(false, true);

        }

        @Override
        public void migrationFinished(MigrationState state) {
            completed.compareAndSet(false, true);
        }

        @Override
        public void replicaMigrationCompleted(ReplicaMigrationEvent event) {
        }

        @Override
        public void replicaMigrationFailed(ReplicaMigrationEvent event) {
        }
    }
}
