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

package com.hazelcast.jet.impl;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.impl.execution.SnapshotRecord;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.core.JetTestSupport.assertTrueEventually;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
public class SnapshotRepositoryTest {

    private static JetTestInstanceFactory factory = new JetTestInstanceFactory();
    private static JetInstance instance;

    @BeforeClass
    public static void beforeClass() {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().addEventJournalConfig(new EventJournalConfig().setMapName("map"));
        instance = factory.newMember(config);
    }

    @AfterClass
    public static void afterClass() {
        factory.shutdownAll();
    }

    @Test
    public void test_getAllSnapshotRecordsFromClient() {
        JetInstance client = factory.newClient();
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.mapJournal("map", JournalInitialPosition.START_FROM_OLDEST))
         .drainTo(Sinks.logger());
        Job job = instance.newJob(p, new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(100));
        SnapshotRepository snapshotRepository = new SnapshotRepository(client);
        assertTrueEventually(() -> assertFalse(snapshotRepository.getAllSnapshotRecords(job.getId()).isEmpty()));
        client.shutdown();
    }

    @Test
    public void when_deleteDeletedSnapshot_then_shouldNotFail() {
        SnapshotRepository repo = new SnapshotRepository(instance);
        final IMap<Long, SnapshotRecord> snapshotMap = repo.getSnapshotMap(1);
        SnapshotRecord record = new SnapshotRecord(1, 1, asList("v1", "v2"));

        // Then - the call should not fail
        repo.deleteSnapshot(snapshotMap, record);
    }
}
