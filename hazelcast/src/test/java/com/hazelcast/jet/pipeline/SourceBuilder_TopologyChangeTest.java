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

package com.hazelcast.jet.pipeline;

import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SourceBuilder_TopologyChangeTest extends JetTestSupport {

    private static volatile boolean stateRestored;

    @Test
    public void test_restartJob_nodeShutDown() {
        testTopologyChange(() -> createHazelcastInstance(), node -> node.shutdown(), true);
    }

    @Test
    public void test_restartJob_nodeTerminated() {
        testTopologyChange(() -> createHazelcastInstance(), node -> node.getLifecycleService().terminate(),
                false);
    }

    @Test
    public void test_restartJob_nodeAdded() {
        testTopologyChange(() -> null, ignore -> createHazelcastInstance(), true);
    }

    private void testTopologyChange(
            Supplier<HazelcastInstance> secondMemberSupplier,
            Consumer<HazelcastInstance> changeTopologyFn,
            boolean assertMonotonicity) {
        stateRestored = false;
        StreamSource<Integer> source = SourceBuilder
                .timestampedStream("src", ctx -> new NumberGeneratorContext())
                .<Integer>fillBufferFn((src, buffer) -> {
                    long expectedCount = NANOSECONDS.toMillis(System.nanoTime() - src.startTime);
                    expectedCount = Math.min(expectedCount, src.current + 100);
                    while (src.current < expectedCount) {
                        buffer.add(src.current, src.current);
                        src.current++;
                    }
                })
                .createSnapshotFn(src -> {
                    System.out.println("Will save " + src.current + " to snapshot");
                    return src;
                })
                .restoreSnapshotFn((src, states) -> {
                    stateRestored = true;
                    assert states.size() == 1;
                    src.restore(states.get(0));
                    System.out.println("Restored " + src.current + " from snapshot");
                })
                .build();

        Config config = smallInstanceConfig();
        config.getJetConfig().setScaleUpDelayMillis(1000); // restart sooner after member add
        HazelcastInstance hz = createHazelcastInstance(config);
        HazelcastInstance possibleSecondNode = secondMemberSupplier.get();

        long windowSize = 100;
        IList<WindowResult<Long>> result = hz.getList("result-" + UuidUtil.newUnsecureUuidString());

        Pipeline p = Pipeline.create();
        p.readFrom(source)
                .withNativeTimestamps(0)
                .window(tumbling(windowSize))
                .aggregate(AggregateOperations.counting())
                .peek()
                .writeTo(Sinks.list(result));

        Job job = hz.getJet().newJob(p, new JobConfig().setProcessingGuarantee(EXACTLY_ONCE).setSnapshotIntervalMillis(500));
        assertTrueEventually(() -> assertFalse("result list is still empty", result.isEmpty()));
        assertJobStatusEventually(job, JobStatus.RUNNING);
        JobRepository jr = new JobRepository(hz);
        waitForFirstSnapshot(jr, job.getId(), 10, false);

        assertFalse(stateRestored);
        changeTopologyFn.accept(possibleSecondNode);
        assertTrueEventually(() -> assertTrue("restoreSnapshotFn was not called", stateRestored));

        // wait until more results are added
        int oldSize = result.size();
        assertTrueEventually(() -> assertTrue("no more results added to the list", result.size() > oldSize));

        cancelAndJoin(job);

        // results should contain sequence of results, each with count=windowSize, monotonic, if job was
        // allowed to terminate gracefully
        Iterator<WindowResult<Long>> iterator = result.iterator();
        for (int i = 0; i < result.size(); i++) {
            WindowResult<Long> next = iterator.next();
            assertEquals(windowSize, (long) next.result());
            if (assertMonotonicity) {
                assertEquals(i * windowSize, next.start());
            }
        }
    }

    private static final class NumberGeneratorContext implements Serializable {

        long startTime = System.nanoTime();
        int current;

        void restore(NumberGeneratorContext other) {
            this.startTime = other.startTime;
            this.current = other.current;
        }
    }
}
