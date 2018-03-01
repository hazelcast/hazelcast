/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JobRestartWithSnapshotTest.SequencesInPartitionsMetaSupplier;
import com.hazelcast.jet.impl.SnapshotRepository;
import com.hazelcast.jet.impl.execution.SnapshotRecord;
import com.hazelcast.jet.impl.execution.SnapshotRecord.SnapshotStatus;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.Serializable;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.TestUtil.throttle;
import static com.hazelcast.jet.core.processor.DiagnosticProcessors.peekOutputP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeMapP;
import static com.hazelcast.jet.impl.SnapshotRepository.snapshotsMapName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class SnapshotFailureTest extends JetTestSupport {

    private static final int LOCAL_PARALLELISM = 4;

    private static volatile boolean storeFailed;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JetInstance instance1;

    @Before
    public void setup() {
        JetConfig config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);

        // force snapshots to fail by adding a failing map store configuration for snapshot data maps
        MapConfig mapConfig = new MapConfig(SnapshotRepository.SNAPSHOT_DATA_NAME_PREFIX + '*');
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new FailingMapStore());
        config.getHazelcastConfig().addMapConfig(mapConfig);

        config.getHazelcastConfig().addEventJournalConfig(new EventJournalConfig()
                .setMapName(SnapshotRepository.SNAPSHOT_NAME_PREFIX + '*'));

        JetInstance[] instances = createJetMembers(config, 2);
        instance1 = instances[0];
    }

    @Test
    public void when_snapshotFails_then_jobShouldNotFail() {
        int numPartitions = 2;
        int numElements = 10;
        IMapJet<Object, Object> results = instance1.getMap("results");

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

        // let's start a second job that will watch the snapshots map and write failed
        // SnapshotRecords to a list, which we will check for presence of failed snapshot
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.mapJournal(snapshotsMapName(job.getId()),
                event -> event.getNewValue() instanceof SnapshotRecord
                        && ((SnapshotRecord) event.getNewValue()).status() == SnapshotStatus.FAILED,
                EventJournalMapEvent::getNewValue,
                JournalInitialPosition.START_FROM_OLDEST))
         .peek()
         .drainTo(Sinks.list("failed_snapshot_records"));
        instance1.newJob(p);

        job.join();

        assertEquals("numPartitions", numPartitions, results.size());
        assertEquals("offset partition 0", numElements - 1, results.get(0));
        assertEquals("offset partition 1", numElements - 1, results.get(1));
        assertTrue("no failure occurred in store", storeFailed);
        assertFalse("no failed snapshot appeared in snapshotsMap", instance1.getList("failed_snapshot_records").isEmpty());
    }

    public static class FailingMapStore extends AMapStore implements Serializable {
        @Override
        public void store(Object o, Object o2) {
            storeFailed = true;
            throw new UnsupportedOperationException();
        }
    }
}
