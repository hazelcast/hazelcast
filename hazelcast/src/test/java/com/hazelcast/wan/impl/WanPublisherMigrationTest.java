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

package com.hazelcast.wan.impl;

import com.hazelcast.config.AbstractWanPublisherConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanEvent;
import com.hazelcast.wan.WanMigrationAwarePublisher;
import com.hazelcast.wan.WanPublisher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getPartitionService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for custom WAN publisher implementation migration support.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanPublisherMigrationTest extends HazelcastTestSupport {

    @Parameter
    public Boolean failMigrations;

    @Parameters(name = "failMigrations:{0}")
    public static Collection<Object[]> data() {
        return asList(new Object[][]{
                {true},
                {false},
        });
    }

    private TestHazelcastInstanceFactory factory;

    @Before
    public void setUp() {
        factory = createHazelcastInstanceFactory(2);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void testMigration() {
        HazelcastInstance[] instances = factory.newInstances(getConfig());
        HazelcastInstance member1 = instances[0];
        HazelcastInstance member2 = instances[1];

        IMap<Object, Object> map = member1.getMap("dummyMap");
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }

        int partitionsToMigrate = 0;
        for (IPartition partition : getPartitionService(member1).getPartitions()) {
            if (partition.isLocal()) {
                partitionsToMigrate++;
            }
        }

        member1.shutdown();
        assertClusterSizeEventually(1, member2);
        MigrationCountingWanPublisher publisher = getPublisher(member2);

        if (!failMigrations) {
            assertEquals(exceptionMsg("migrationStart", publisher),
                    partitionsToMigrate, publisher.migrationStart.intValue());
            // it may happen that we have additional partition anti-entropy operations
            // that call process but don't show up as migration operations
            assertTrue("Expected at least " + partitionsToMigrate
                            + " migration operations to be processed but was " + publisher,
                    publisher.migrationProcess.intValue() >= partitionsToMigrate);
            assertEquals(exceptionMsg("migrationCommit", publisher),
                    partitionsToMigrate, publisher.migrationCommit.intValue());
        } else {
            assertEquals(exceptionMsg("migrationStart", publisher),
                    partitionsToMigrate + 1, publisher.migrationStart.intValue());
            // it may happen that we have additional partition anti-entropy operations
            // that call process but don't show up as migration operations
            assertTrue("Expected at least " + partitionsToMigrate
                            + " migration operations to be processed but was " + publisher,
                    publisher.migrationProcess.intValue() >= partitionsToMigrate);
            assertEquals(exceptionMsg("migrationCommit", publisher),
                    partitionsToMigrate, publisher.migrationCommit.intValue());
            assertEquals(exceptionMsg("migrationRollback", publisher),
                    1, publisher.migrationRollback.intValue());
        }
    }

    @Nonnull
    private static String exceptionMsg(String counterName, MigrationCountingWanPublisher publisher) {
        return "not expected " + counterName + " count (" + publisher.toString() + ")";
    }

    @Override
    protected Config getConfig() {
        WanCustomPublisherConfig publisherConfig = new WanCustomPublisherConfig()
                .setPublisherId("dummyPublisherId")
                .setClassName(MigrationCountingWanPublisher.class.getName());
        publisherConfig.getProperties().put("failMigrations", failMigrations);

        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig()
                .setName("dummyWan")
                .addCustomPublisherConfig(publisherConfig);

        WanReplicationRef wanRef = new WanReplicationRef()
                .setName("dummyWan")
                .setMergePolicyClassName(PassThroughMergePolicy.class.getName());

        MapConfig mapConfig = new MapConfig("default")
                .setWanReplicationRef(wanRef);

        return smallInstanceConfig()
                .addWanReplicationConfig(wanReplicationConfig)
                .addMapConfig(mapConfig);
    }

    private MigrationCountingWanPublisher getPublisher(HazelcastInstance instance) {
        WanReplicationService service = getNodeEngineImpl(instance).getWanReplicationService();
        DelegatingWanScheme delegate = service.getWanReplicationPublishers("dummyWan");
        return (MigrationCountingWanPublisher) delegate.getPublishers().iterator().next();
    }

    public static class MigrationCountingWanPublisher implements
            WanPublisher, WanMigrationAwarePublisher<Map<String, String>> {
        private final AtomicBoolean failMigration = new AtomicBoolean();

        final AtomicLong migrationStart = new AtomicLong();
        final AtomicLong migrationCommit = new AtomicLong();
        final AtomicLong migrationRollback = new AtomicLong();
        final AtomicLong migrationProcess = new AtomicLong();

        @Override
        public void init(WanReplicationConfig wanReplicationConfig, AbstractWanPublisherConfig publisherConfig) {
            Boolean failMigrations = (Boolean) publisherConfig.getProperties().get("failMigrations");
            failMigration.set(failMigrations);
        }

        @Override
        public void shutdown() {

        }

        @Override
        public void doPrepublicationChecks() {

        }

        @Override
        public void publishReplicationEvent(WanEvent eventObject) {

        }

        @Override
        public void publishReplicationEventBackup(WanEvent eventObject) {

        }

        @Override
        public void onMigrationStart(PartitionMigrationEvent event) {
            migrationStart.incrementAndGet();
            if (failMigration.compareAndSet(true, false)) {
                throw new RuntimeException("Intentionally failing migration");
            }
        }

        @Override
        public void onMigrationCommit(PartitionMigrationEvent event) {
            migrationCommit.incrementAndGet();
        }

        @Override
        public void onMigrationRollback(PartitionMigrationEvent event) {
            migrationRollback.incrementAndGet();
        }

        @Override
        public Map<String, String> prepareEventContainerReplicationData(PartitionReplicationEvent event,
                                                                        Collection<ServiceNamespace> collection) {
            Map<String, String> container = new HashMap<>();
            container.put("test", "value");
            return container;
        }

        @Override
        public void processEventContainerReplicationData(int partitionId, Map<String, String> eventContainer) {
            assertNotNull(eventContainer);
            assertEquals(1, eventContainer.size());
            assertEquals("value", eventContainer.get("test"));
            migrationProcess.incrementAndGet();
        }

        @Override
        public void collectAllServiceNamespaces(PartitionReplicationEvent event,
                                                Set<ServiceNamespace> set) {
            set.add(new DistributedObjectNamespace(MapService.SERVICE_NAME, "testMap"));
        }

        @Override
        public String toString() {
            return "MigrationCountingWanPublisher{"
                    + "failMigration=" + failMigration
                    + ", migrationStart=" + migrationStart
                    + ", migrationCommit=" + migrationCommit
                    + ", migrationRollback=" + migrationRollback
                    + ", migrationProcess=" + migrationProcess
                    + '}';
        }
    }
}
