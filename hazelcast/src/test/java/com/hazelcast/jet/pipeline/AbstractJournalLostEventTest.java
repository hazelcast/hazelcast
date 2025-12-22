/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobAssertions;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import java.util.List;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static org.assertj.core.api.Assertions.assertThat;

@Category({SlowTest.class})
public abstract class AbstractJournalLostEventTest extends PipelineTestSupport {
    protected static HazelcastInstance remoteInstance;
    protected static ClientConfig remoteHzClientConfig;
    private static List<HazelcastInstance> remoteCluster;

    @Before
    public void setup() {
        member.getList(sinkName).clear();
    }

    @BeforeClass
    public static void setupRemote() {
        var remoteClusterConfig = prepareConfig();
        remoteClusterConfig.setClusterName(randomName());
        remoteCluster = createRemoteCluster(remoteClusterConfig, MEMBER_COUNT);
        remoteInstance = remoteCluster.get(0);
        remoteHzClientConfig = getClientConfigForRemoteCluster(remoteInstance);
    }

    @AfterClass
    public static void shutdownRemote() {
        remoteCluster.forEach(HazelcastInstance::shutdown);
    }

    protected <T> void performTest(
            HazelcastInstance producerInstance,
            StreamSource<T> streamSource,
            FunctionEx<T, Boolean> mapFn,
            EventFilterType filter
    ) {
        Pipeline p = Pipeline.create();

        p.readFrom(streamSource)
                .withTimestamps(e -> System.currentTimeMillis(), 0)
                .map(mapFn::apply)
                .writeTo(Sinks.list(sinkName));

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(EXACTLY_ONCE);
        var job = hz().getJet().newJob(p, config);

        JobAssertions.assertThat(job).eventuallyHasStatus(RUNNING);

        int counter = 0;
        put(producerInstance, 0, counter);
        remove(producerInstance, 0);
        counter = 2;

        List<Boolean> sinkList = member.getList(sinkName);
        assertTrueEventually(() -> assertThat(sinkList.size()).isGreaterThan(0));

        job.suspend();
        JobAssertions.assertThat(job).eventuallyHasStatus(SUSPENDED);

        // Fill up the journal to cause lost events
        while (counter < 2 * JOURNAL_CAPACITY_PER_PARTITION) {
            put(producerInstance, 0, counter);
            remove(producerInstance, 0);
            counter += 2;
        }

        job.resume();
        JobAssertions.assertThat(job).eventuallyHasStatus(RUNNING);

        var expectedOutputSize = switch (filter) {
            case ALL -> 2 + JOURNAL_CAPACITY_PER_PARTITION;
            case ONLY_PUT, ONLY_REMOVE -> 1 + JOURNAL_CAPACITY_PER_PARTITION / 2;
        };

        assertTrueEventually(() -> assertThat(sinkList.size()).isEqualTo(expectedOutputSize));

        assertThat(sinkList).containsOnlyOnce(true);
    }

    protected <T> void performRareEventTest(StreamSource<T> streamSource) {
        Pipeline p = Pipeline.create();
        p.readFrom(streamSource)
                .withTimestamps(e -> System.currentTimeMillis(), 0)
                .writeTo(Sinks.list(sinkName));

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(EXACTLY_ONCE);
        var job = hz().getJet().newJob(p, config);
        JobAssertions.assertThat(job).eventuallyHasStatus(RUNNING);

        put(member, 0, 0);
        remove(member, 0);

        List<Boolean> sinkList = member.getList(sinkName);
        assertTrueEventually(() -> assertThat(sinkList).hasSize(1));

        job.suspend();
        JobAssertions.assertThat(job).eventuallyHasStatus(SUSPENDED);
        sinkList.clear();

        // Fill up the journal to cause lost events
        for (int i = 0; i < 2 * JOURNAL_CAPACITY_PER_PARTITION; i++) {
            put(member, 0, i);
        }

        job.resume();
        JobAssertions.assertThat(job).eventuallyHasStatus(RUNNING);

        // Normal processing of an event that is filtered out
        for (int i = 0; i < JOURNAL_CAPACITY_PER_PARTITION; i++) {
            put(member, 0, i);
        }

        assertThat(sinkList).isEmpty();
        remove(member, 0);

        assertTrueEventually(() -> assertThat(sinkList.size()).isEqualTo(1));
        assertThat(sinkList.get(0)).isTrue();
    }


    protected abstract void put(HazelcastInstance instance, Integer key, Integer value);

    protected abstract void remove(HazelcastInstance instance, Integer key);

    protected enum EventFilterType {
        ALL,
        ONLY_PUT,
        ONLY_REMOVE
    }
}
