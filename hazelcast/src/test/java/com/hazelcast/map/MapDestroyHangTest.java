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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.diagnostics.HealthMonitorLevel;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalMigrationListener;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.HdrHistogram.Histogram;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
@Ignore
public class MapDestroyHangTest extends HazelcastTestSupport {

    MasterMigrationListener migrationListener = new MasterMigrationListener();
    TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
    AtomicLong total = new AtomicLong();
    final AtomicBoolean stop = new AtomicBoolean();

    @Before
    public void setUp() {

//        Node node = HazelcastTestSupport.getNode(instance1);
//        node.partitionService.setInternalMigrationListener(migrationListener);
    }

    @After
    public void tearDown() throws Exception {
        long nanos = total.get();
        System.err.println(TimeUnit.NANOSECONDS.toMillis(nanos));
    }

    private class MasterMigrationListener extends InternalMigrationListener {
        // InternalMigrationListener is used by single thread on master member.
        private final Histogram histogram = new Histogram(3);
        private volatile long startTime = -1;

        @Override
        public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
            if (participant != MigrationParticipant.MASTER) {
                return;
            }

            if (startTime == -1) {
                startTime = System.nanoTime();
                return;
            }

            long diff = System.nanoTime() - startTime;
            //histogram.recordValue(diff);
            startTime = System.nanoTime();

            total.addAndGet(diff);
        }
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.setProperty(GroupProperty.GENERIC_OPERATION_THREAD_COUNT.getName(), "1");
        config.setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "2");
        config.setProperty(GroupProperty.HEALTH_MONITORING_LEVEL.getName(), HealthMonitorLevel.OFF.toString());
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "103");
        config.setProperty("hazelcast.slow.operation.detector.stacktrace.logging.enabled", "true");
        return config;
    }

    @Test(timeout = 1000 * 60 * 10)
    public void destroyAllReplicasIncludingBackups() throws InterruptedException {
        final HazelcastInstance instance1 = factory.newHazelcastInstance(getConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(getConfig());
        final HazelcastInstance instance3 = factory.newHazelcastInstance(getConfig());

        Thread mapFactory = new Thread() {
            @Override
            public void run() {
                while (!stop.get()) {
                    final IMap<Integer, Integer> map = instance1.getMap(randomMapName());
                    for (int i = 0; i < 100; i++) {
                        map.put(i, i);
                    }

                    sleepMillis(100);
                }
            }
        };

        Thread destroyer = new Thread() {
            @Override
            public void run() {
                while (!stop.get()) {
                    Collection<DistributedObject> distributedObjects = instance1.getDistributedObjects();
                    for (DistributedObject distributedObject : distributedObjects) {
                        distributedObject.destroy();
                    }
                }
            }
        };

        Thread shadowMember = new Thread() {
            @Override
            public void run() {
                instance3.shutdown();
                sleepSeconds(2);
                factory.newHazelcastInstance(getConfig());
            }
        };

        mapFactory.start();
        destroyer.start();
        shadowMember.start();

        sleepSeconds((int) TimeUnit.MINUTES.toSeconds(10));
        stop.set(true);

        mapFactory.join();
        destroyer.join();
        shadowMember.join();


    }

    private void assertAllPartitionContainersAreEmptyEventually(final HazelcastInstance instance) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertAllPartitionContainersAreEmpty(instance);
            }
        });
    }

    private void assertAllPartitionContainersAreEmpty(HazelcastInstance instance) {
        MapServiceContext context = getMapServiceContext(instance);
        int partitionCount = getPartitionCount(instance);

        for (int i = 0; i < partitionCount; i++) {
            PartitionContainer container = context.getPartitionContainer(i);
            ConcurrentMap<String, RecordStore> maps = container.getMaps();
            assertTrue(maps.isEmpty());
        }
    }

    private MapServiceContext getMapServiceContext(HazelcastInstance instance) {
        NodeEngineImpl nodeEngine1 = getNodeEngineImpl(instance);
        MapService mapService = nodeEngine1.getService(MapService.SERVICE_NAME);
        return mapService.getMapServiceContext();
    }

    private int getPartitionCount(HazelcastInstance instance) {
        Node node = getNode(instance);
        return node.getProperties().getInteger(GroupProperty.PARTITION_COUNT);
    }
}
