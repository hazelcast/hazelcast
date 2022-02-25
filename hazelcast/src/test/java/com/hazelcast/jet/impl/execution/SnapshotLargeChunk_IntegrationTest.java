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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.SnapshotValidationRecord;
import com.hazelcast.jet.impl.util.AsyncSnapshotWriterImpl;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.IntSummaryStatistics;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.stream.Collectors.summarizingInt;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SnapshotLargeChunk_IntegrationTest extends JetTestSupport {

    @Test
    public void test_snapshotRestoreLargeChunk() {
        HazelcastInstance instance = createHazelcastInstance();
        DAG dag = new DAG();
        dag.newVertex("src", LargeStateP::new).localParallelism(1);
        Job job = instance.getJet().newJob(dag, new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(DAYS.toMillis(1)));

        assertJobStatusEventually(job, RUNNING);
        job.restart();
        assertJobStatusEventually(job, RUNNING);

        // assert that the snapshot exists and that the chunk was large enough
        IMap<Object, Object> map = instance.getMap(JobRepository.snapshotDataMapName(job.getId(), 0));
        SnapshotValidationRecord validationRec = (SnapshotValidationRecord) map.get(SnapshotValidationRecord.KEY);
        assertEquals(1, validationRec.numChunks());
        IntSummaryStatistics stats = map.values().stream()
                                          .filter(v -> v instanceof byte[])
                                          .collect(summarizingInt(v -> ((byte[]) v).length));
        assertTrue("min=" + stats.getMin(), stats.getMin() > AsyncSnapshotWriterImpl.DEFAULT_CHUNK_SIZE);
    }

    /**
     * Processor that saves large value to the key and asserts it when restoring.
     */
    private static final class LargeStateP extends AbstractProcessor {

        private final int[] data = IntStream.range(0, AsyncSnapshotWriterImpl.DEFAULT_CHUNK_SIZE / Bits.INT_SIZE_IN_BYTES)
                                            .toArray();

        @Override
        public boolean complete() {
            return false;
        }

        @Override
        public boolean saveToSnapshot() {
            return tryEmitToSnapshot("key", data);
        }

        @Override
        protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
            assertEquals("key", key);
            assertArrayEquals(data, (int[]) value);
        }
    }
}
