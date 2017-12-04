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
import com.hazelcast.jet.impl.SnapshotRepository;
import com.hazelcast.jet.impl.execution.SnapshotRecord;
import com.hazelcast.jet.impl.execution.SnapshotRecord.SnapshotStatus;
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
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.TestUtil.throttle;
import static com.hazelcast.jet.core.processor.DiagnosticProcessors.peekOutputP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeMapP;
import static com.hazelcast.jet.impl.SnapshotRepository.snapshotsMapName;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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

    @Before
    public void setup() {
        factory = new JetTestInstanceFactory();

        JetConfig config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);

        // force snapshots to fail by adding a failing map store configuration for snapshot data maps
        MapConfig mapConfig = new MapConfig(SnapshotRepository.SNAPSHOT_DATA_NAME_PREFIX + '*');
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new FailingMapStore());
        config.getHazelcastConfig().addMapConfig(mapConfig);

        JetInstance[] instances = factory.newMembers(config, 2);
        instance1 = instances[0];
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void when_snapshotFails_then_jobShouldNotFail() throws Exception {
        int numPartitions = 2;
        int numElements = 10;
        IStreamMap<Object, Object> results = instance1.getMap("results");

        DAG dag = new DAG();
        SequencesInPartitionsMetaSupplier sup = new SequencesInPartitionsMetaSupplier(numPartitions, numElements);
        Vertex generator = dag.newVertex("generator", peekOutputP(throttle(sup, 2)))
                              .localParallelism(1);
        Vertex writeMap = dag.newVertex("writeMap", writeMapP(results.getName())).localParallelism(1);
        dag.edge(between(generator, writeMap));

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        config.setSnapshotIntervalMillis(100);

        Job job = instance1.newJob(dag, config);

        IMap<Object, Object> snapshotsMap = instance1.getMap(snapshotsMapName(job.getJobId()));

        SnapshotRecord[] failedRecord = new SnapshotRecord[1];
        while (failedRecord[0] == null && !job.getFuture().isDone()) {
            // find a failed record in snapshots map
            snapshotsMap
                    .values().stream()
                    .filter(r -> r instanceof SnapshotRecord)
                    .map(r -> (SnapshotRecord) r)
                    .filter(r -> r.status() == SnapshotStatus.FAILED)
                    .findFirst()
                    .ifPresent(r -> failedRecord[0] = r);
            LockSupport.parkNanos(MILLISECONDS.toNanos(1));
        }

        job.join();

        assertEquals("numPartitions", numPartitions, results.size());
        assertEquals("offset partition 0", numElements - 1, results.get(0));
        assertEquals("offset partition 1", numElements - 1, results.get(1));
        assertTrue("no failure occurred in store", storeFailed);
        assertNotNull("no failed snapshot appeared in snapshotsMap", failedRecord[0]);
    }

    public static class FailingMapStore extends AMapStore implements Serializable {
        @Override
        public void store(Object o, Object o2) {
            storeFailed = true;
            throw new UnsupportedOperationException();
        }
    }
}
