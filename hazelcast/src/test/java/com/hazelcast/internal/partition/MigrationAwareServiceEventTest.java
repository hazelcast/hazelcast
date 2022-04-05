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

package com.hazelcast.internal.partition;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationResponseHandler;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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
        ConfigAccessor.getServicesConfig(config).addServiceConfig(serviceConfig);

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

        assertThat(responseHandler.failures, Matchers.empty());
    }

    private Config newConfig(FailingOperationResponseHandler responseHandler) {
        Config config = new Config();
        ConfigAccessor.getServicesConfig(config).addServiceConfig(
                new ServiceConfig().setEnabled(true).setImplementation(new MigrationCommitRollbackTestingService(responseHandler))
                        .setName(MigrationCommitRollbackTestingService.NAME));
        return config;
    }

    private static class FailingOperationResponseHandler implements OperationResponseHandler {
        private final Queue<String> failures = new ConcurrentLinkedQueue<String>();

        @Override
        public void sendResponse(Operation operation, Object response) {
            assert operation instanceof DummyPartitionAwareOperation : "Invalid operation: " + operation;
            NodeEngine nodeEngine = operation.getNodeEngine();
            if (!(response instanceof RetryableHazelcastException) && nodeEngine.isRunning()) {
                DummyPartitionAwareOperation op = (DummyPartitionAwareOperation) operation;
                failures.add("Unexpected response: " + response + ". Node: " + nodeEngine.getThisAddress()
                        + ", Event: " + op.event + ", Type: " + op.type);
            }
        }
    }

    private static class MigrationCommitRollbackTestingService implements MigrationAwareService, ManagedService {
        private static final String NAME = MigrationCommitRollbackTestingService.class.getSimpleName();
        private static final String TYPE_COMMIT = "COMMIT";
        private static final String TYPE_ROLLBACK = "ROLLBACK";

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
            checkPartition(event, TYPE_COMMIT);
        }

        @Override
        public void rollbackMigration(PartitionMigrationEvent event) {
            checkPartition(event, TYPE_ROLLBACK);
        }

        private void checkPartition(PartitionMigrationEvent event, String type) {
            if (event.getNewReplicaIndex() != 0 && event.getCurrentReplicaIndex() != 0) {
                return;
            }

            checkPartitionMigrating(event, type);

            if (event.getCurrentReplicaIndex() != -1) {
                runPartitionOperation(event, type, event.getCurrentReplicaIndex());
            }
            if (event.getNewReplicaIndex() != -1) {
                runPartitionOperation(event, type, event.getNewReplicaIndex());
            }
        }

        private void runPartitionOperation(PartitionMigrationEvent event, String type, int replicaIndex) {
            DummyPartitionAwareOperation op = new DummyPartitionAwareOperation(event, type);
            op.setNodeEngine(nodeEngine).setPartitionId(event.getPartitionId()).setReplicaIndex(replicaIndex);
            op.setOperationResponseHandler(responseHandler);
            nodeEngine.getOperationService().run(op);
        }

        private void checkPartitionMigrating(PartitionMigrationEvent event, String type) {
            InternalPartitionServiceImpl partitionService =
                    (InternalPartitionServiceImpl) nodeEngine.getPartitionService();

            InternalPartition partition = partitionService.getPartition(event.getPartitionId());
            if (!partition.isMigrating() && nodeEngine.isRunning()) {
                responseHandler.failures.add("Migrating flag is not set. Node: " + nodeEngine.getThisAddress()
                        + ", Event: " + event + ", Type: " + type);
            }
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
        private final String type;

        DummyPartitionAwareOperation(PartitionMigrationEvent event, String type) {
            this.event = event;
            this.type = type;
        }

        @Override
        public void run() throws Exception {
        }

        @Override
        public Object getResponse() {
            return Boolean.TRUE;
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
