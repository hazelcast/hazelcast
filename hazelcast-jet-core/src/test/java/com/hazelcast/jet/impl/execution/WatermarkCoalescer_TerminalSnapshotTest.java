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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.spi.properties.GroupProperty;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WatermarkCoalescer_TerminalSnapshotTest extends JetTestSupport {

    private static final int PARTITION_COUNT = 2;
    private static final int COUNT = 10;

    private JetInstance instance;
    private IMap<String, Integer> sourceMap;

    @Before
    public void setUp() {
        JetConfig config = new JetConfig();
        EventJournalConfig journalConfig = new EventJournalConfig()
                .setMapName("*")
                .setCapacity(1_000_000)
                .setEnabled(true);

        // number of partitions must match number of source processors for coalescing
        // to work correctly
        config.getHazelcastConfig().setProperty(
                GroupProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));
        config.getHazelcastConfig().addEventJournalConfig(journalConfig);
        instance = createJetMember(config);
        sourceMap = instance.getMap("test");
    }

    @Test
    public void test() throws Exception {
        /*
        This test tests the issue that after a terminal barrier is processed, no other work should
        be done by the ProcessorTasklet or CIES after that (except for emitting the DONE_ITEM).
        Also, if at-least-once guarantee is used, the tasklet should not continue to drain
        the queue that had the barrier while waiting for other barriers.

        Specifically, the issue was that in at-least-once mode the DONE_ITEM was processed
        after the terminal barrier while waiting for the barrier on other queues/edges. The
        DONE_ITEM could have caused a WM being emitted after the barrier, which is ok
        for the at-least-once mode, but the terminal snapshot should behave as if exactly-once
        mode was used.

        This test ensures that we're waiting for a WM in coalescer (by having a stream skew)
        and then does a graceful restart in at-least-once mode and checks that the results are
        correct.
         */
        String key0 = generateKeyForPartition(instance.getHazelcastInstance(), 0);
        String key1 = generateKeyForPartition(instance.getHazelcastInstance(), 1);

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.mapJournal(sourceMap, JournalInitialPosition.START_FROM_OLDEST))
                .setLocalParallelism(PARTITION_COUNT)
                .addTimestamps(Map.Entry::getValue, 0)
                .groupingKey(Map.Entry::getKey)
                .window(WindowDefinition.sliding(1, 1))
                .aggregate(AggregateOperations.counting()).setLocalParallelism(PARTITION_COUNT)
                .drainTo(SinkBuilder.sinkBuilder("throwing", ctx -> "").
                        <TimestampedEntry<String, Long>>receiveFn((w, e) -> {
                            if (e.getValue() != COUNT) {
                                throw new RuntimeException("Received unexpected item " + e + ", expected count is "
                                        + COUNT);
                            }
                        }).build());

        Job job = instance.newJob(p, new JobConfig().setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE));

        List<Future> futures = new ArrayList<>();
        futures.add(spawn(() -> {
            for (;;) {
                assertJobStatusEventually(job, JobStatus.RUNNING);
                System.out.println("============RESTARTING JOB=========");
                job.restart();
                Thread.sleep(2000);
            }
        }));

        // one producer is twice as fast as the other, to cause waiting for WM while doing snapshot
        futures.add(spawn(() -> producer(key0, 1)));
        futures.add(spawn(() -> producer(key1, 2)));

        sleepSeconds(20);
        for (Future f : futures) {
            f.cancel(true);
            // check that the future was cancelled and didn't fail with another error
            try {
                f.get();
                fail("Exception was expected");
            } catch (CancellationException expected) {
            }
        }

        // check that the job is running
        JobStatus status = job.getStatus();
        assertTrue("job should not be completed, status=" + status,
                status != FAILED && status != COMPLETED && status != SUSPENDED);
    }

    private void producer(String key, int delayMs) {
        for (int ts = 0; ; ts++) {
            for (int j = 0; j < COUNT; j++) {
                sourceMap.set(key, ts);
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(delayMs));
        }
    }
}
