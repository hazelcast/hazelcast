/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MigrationAwareServiceEventTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory();
    }

    @Test
    public void migrationCommitEvents_shouldBeEqual_onSource_and_onDestination() throws Exception {
        Config config = new Config();
        final MigrationEventCounterService counter = new MigrationEventCounterService();
        ServiceConfig serviceConfig = new ServiceConfig()
                .setEnabled(true).setName("event-counter")
                .setImplementation(counter);
        config.getServicesConfig().addServiceConfig(serviceConfig);

        final HazelcastInstance hz = factory.newHazelcastInstance(config);
        warmUpPartitions(hz);

        final AssertTask assertTask = new AssertTask() {
            final InternalPartitionService partitionService = getNode(hz).getPartitionService();

            @Override
            public void run() throws Exception {
                assertEquals(0, partitionService.getMigrationQueueSize());
                final int source = counter.sourceCommits.get();
                final int destination = counter.destinationCommits.get();
                assertEquals(source, destination);
            }
        };

        factory.newHazelcastInstance(config);
        assertTrueEventually(assertTask);

        factory.newHazelcastInstance(config);
        assertTrueEventually(assertTask);
    }

    @Test
    public void partitionIsMigratingFlag_shouldBeSet_until_serviceCommitRollback_isCompleted() throws Exception {
        FailingOperationResponseHandler responseHandler = new FailingOperationResponseHandler();
        HazelcastInstance hz = factory.newHazelcastInstance(newConfig(responseHandler));
        warmUpPartitions(hz);

        HazelcastInstance[] instances = new HazelcastInstance[2];
        for (int i = 0; i < instances.length; i++) {
            instances[i] = factory.newHazelcastInstance(newConfig(responseHandler));
        }
        waitAllForSafeState(instances);
        for (HazelcastInstance instance : instances) {
            instance.getLifecycleService().terminate();
        }
        waitAllForSafeState(hz);

        Queue<Object> responses = responseHandler.responses;
        assertTrue("Unexpected responses: " + responses, responses.isEmpty());
    }

    private Config newConfig(FailingOperationResponseHandler responseHandler) {
        Config config = new Config();
        config.getServicesConfig().addServiceConfig(
                new ServiceConfig().setEnabled(true).setImplementation(new MigrationCommitRollbackTestingService(responseHandler))
                        .setName(MigrationCommitRollbackTestingService.NAME));
        return config;
    }

    private static class FailingOperationResponseHandler implements OperationResponseHandler {
        private final Queue<Object> responses = new ConcurrentLinkedQueue<Object>();

        @Override
        public void sendResponse(Operation op, Object response) {
            if (!(response instanceof RetryableHazelcastException)) {
                responses.add(response);
                System.err.println("Unexpected response " + response + " from " + op);
            }
        }

        @Override
        public boolean isLocal() {
            return true;
        }
    }

    private static class MigrationCommitRollbackTestingService implements MigrationAwareService, ManagedService {
        private static final String NAME = MigrationCommitRollbackTestingService.class.getSimpleName();
        private final FailingOperationResponseHandler responseHandler;
        private volatile NodeEngine nodeEngine;

        MigrationCommitRollbackTestingService(FailingOperationResponseHandler responseHandler) {
            this.responseHandler = responseHandler;
        }

        @Override
        public void init(NodeEngine nodeEngine, Properties properties) {
            this.nodeEngine = nodeEngine;
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
            executePartitionOperation(event);
        }

        private void executePartitionOperation(PartitionMigrationEvent event) {
            if (event.getNewReplicaIndex() != 0 && event.getCurrentReplicaIndex() != 0) {
                return;
            }

            DummyPartitionAwareOperation op = new DummyPartitionAwareOperation(event);
            op.setPartitionId(event.getPartitionId()).setReplicaIndex(event.getNewReplicaIndex());
            op.setOperationResponseHandler(responseHandler);
            nodeEngine.getOperationService().run(op);
        }

        @Override
        public void rollbackMigration(PartitionMigrationEvent event) {
            executePartitionOperation(event);
        }

        @Override
        public void reset() {
        }

        @Override
        public void shutdown(boolean terminate) {
        }
    }

    private static class DummyPartitionAwareOperation extends Operation {
        private final PartitionMigrationEvent event;
        DummyPartitionAwareOperation(PartitionMigrationEvent event) {
            this.event = event;
        }

        @Override
        public void run() throws Exception {
        }

        @Override
        public Object getResponse() {
            return Boolean.TRUE;
        }

        @Override
        public String toString() {
            return "TestPartitionAwareOperation{" + "event=" + event + '}';
        }
    }

    private static class MigrationEventCounterService implements MigrationAwareService {

        final AtomicInteger sourceCommits = new AtomicInteger();
        final AtomicInteger destinationCommits = new AtomicInteger();

        @Override
        public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
            return null;
        }

        @Override
        public void beforeMigration(PartitionMigrationEvent event) {
        }

        @Override
        public void commitMigration(PartitionMigrationEvent event) {
            // Only count ownership migrations.
            // For missing (new) backups there are also COPY migrations (call it just replication)
            // which don't have a source endpoint.
            if (event.getCurrentReplicaIndex() == 0 || event.getNewReplicaIndex() == 0) {
                if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
                    sourceCommits.incrementAndGet();
                } else {
                    destinationCommits.incrementAndGet();
                }
            }
        }

        @Override
        public void rollbackMigration(PartitionMigrationEvent event) {
        }

    }
}
