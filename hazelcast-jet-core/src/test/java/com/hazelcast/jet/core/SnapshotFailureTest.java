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

package com.hazelcast.jet.core;

import com.hazelcast.client.map.helpers.AMapStore;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JobRestartWithSnapshotTest.SequencesInPartitionsMetaSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.impl.SnapshotRepository;
import com.hazelcast.jet.impl.execution.SnapshotRecord;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.TestUtil.throttle;
import static com.hazelcast.jet.core.processor.DiagnosticProcessors.peekOutput;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class SnapshotFailureTest extends JetTestSupport {

    private static final int LOCAL_PARALLELISM = 4;

    private static volatile boolean storeFailed;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JetInstance instance1;
    private JetTestInstanceFactory factory;
    private JetInstance instance2;

    @Before
    public void setup() {
        factory = new JetTestInstanceFactory();

        JetConfig config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);

        // force snapshots to fail by adding a failing map store configuration for snapshot data maps
        MapConfig mapConfig = new MapConfig(SnapshotRepository.SNAPSHOT_DATA_NAME_PREFIX + "*");
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new FailingMapStore());
        config.getHazelcastConfig().addMapConfig(mapConfig);

        JetInstance[] instances = factory.newMembers(config, 2);
        instance1 = instances[0];
        instance2 = instances[1];
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void when_snapshotFails_then_jobShouldNotFail() throws InterruptedException {
        int numPartitions = 2;
        int numElements = 10;
        IStreamMap<Object, Object> results = instance1.getMap("results");

        DAG dag = new DAG();
        SequencesInPartitionsMetaSupplier sup = new SequencesInPartitionsMetaSupplier(numPartitions, numElements);
        Vertex generator = dag.newVertex("generator", peekOutput(throttle(sup, 2)))
                              .localParallelism(1);
        Vertex writeMap = dag.newVertex("writeMap", SinkProcessors.writeMapP(results.getName())).localParallelism(1);
        dag.edge(between(generator, writeMap));

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        config.setSnapshotIntervalMillis(1200);
        Job job = instance1.newJob(dag, config);

        // Successful snapshot cannot happen in this test
        AtomicInteger jobFinishedLatch = new AtomicInteger(1);
        AtomicReference<SnapshotRecord> successfulRecord = new AtomicReference<>();
        new Thread(() -> {
            IMap<Object, Object> snapshotsMap = instance1.getMap(SnapshotRepository.snapshotsMapName(job.getJobId()));
            while (jobFinishedLatch.get() != 0) {
                snapshotsMap.values().stream()
                            .filter(r -> r instanceof SnapshotRecord && ((SnapshotRecord) r).isSuccessful())
                            .findFirst()
                            .ifPresent(r -> successfulRecord.compareAndSet(null, (SnapshotRecord) r));
                LockSupport.parkNanos(MILLISECONDS.toNanos(1));
            }
        }).start();
        job.join();
        jobFinishedLatch.decrementAndGet();

        assertEquals("numPartitions", numPartitions, results.size());
        assertEquals("offset partition 0", numElements - 1, results.get(0));
        assertEquals("offset partition 1", numElements - 1, results.get(1));
        assertTrue("no failure occurred in store", storeFailed);
        assertNull("successful snapshot appeared in snapshotsMap", successfulRecord.get());
    }

    public static class FailingMapStore extends AMapStore implements Serializable {
        @Override
        public void store(Object o, Object o2) {
            storeFailed = true;
            throw new UnsupportedOperationException();
        }
    }
}
