/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.IList;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.TestProcessors.ListSource;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.spi.CoreService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.TestUtil.throttle;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class HazelcastConnector_RestartTest extends JetTestSupport {

    private JetInstance instance1;
    private JetInstance instance2;

    @Before
    public void setup() {
        JetConfig config = new JetConfig();
        Config hazelcastConfig = config.getHazelcastConfig();
        hazelcastConfig.addCacheConfig(new CacheSimpleConfig().setName("*"));
        hazelcastConfig.addEventJournalConfig(new EventJournalConfig().setCacheName("stream*").setMapName("stream*"));
        config.getHazelcastConfig().getServicesConfig().addServiceConfig(
                new ServiceConfig()
                        .setName("MigrationBlockingService")
                        .setEnabled(true)
                        .setImplementation(new MigrationBlockingService()));

        instance1 = createJetMember(config);
        instance2 = createJetMember(config);
    }

    @Test
    public void when_iListWrittenAndMemberShutdown_then_jobRestarts() throws Exception {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source",
                throttle(() -> new ListSource(range(0, 1000).boxed().collect(toList())), 10));
        Vertex sink = dag.newVertex("sink", writeListP("sink"));
        dag.edge(between(source, sink));
        source.localParallelism(1);

        instance1.newJob(dag, new JobConfig().setAutoScaling(true));
        // wait for the job to start producing
        IList<Integer> sinkList = instance1.getHazelcastInstance().getList("sink");
        assertTrueEventually(() -> assertTrue("no output to sink", sinkList.size() >= 4), 5);

        // When
        // initiate the shutdown. Thanks to the MigrationBlockingService migration will not finish
        // before the latch is counted down.
        Future shutdownFuture = spawn(() -> instance2.shutdown());

        // Then - assert that the job stopped producing output
        sleepSeconds(4);
        assertFalse("node engine is running",
                ((HazelcastInstanceImpl) instance2.getHazelcastInstance()).node.nodeEngine.isRunning());
        int sizeAfterShutdown = sinkList.size();
        assertTrueAllTheTime(() -> assertEquals("output continues after shutdown", sizeAfterShutdown, sinkList.size()), 5);

        // When2 - allow the migration and shutdown to complete
        MigrationBlockingService.migrationDoneLatch.countDown();
        System.out.println("Latch counted down");
        // wait for the shutdown to finish
        shutdownFuture.get();

        // Then2 - job restarts and continues production
        assertTrueEventually(() ->
                assertTrue("no output after migration completed", sinkList.size() > sizeAfterShutdown + 4), 10);
    }

    // A CoreService with a slow post-join op. Its post-join operation will be executed before map's
    // post-join operation so we can ensure indexes are created via MapReplicationOperation,
    // even though PostJoinMapOperation has not yet been executed.
    private static class MigrationBlockingService implements CoreService, MigrationAwareService {
        static CountDownLatch migrationDoneLatch = new CountDownLatch(1);

        @Override
        public void beforeMigration(PartitionMigrationEvent event) {
            try {
                migrationDoneLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
            return null;
        }

        @Override
        public void commitMigration(PartitionMigrationEvent event) {
        }

        @Override
        public void rollbackMigration(PartitionMigrationEvent event) {
        }
    }
}
