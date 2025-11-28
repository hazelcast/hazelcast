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

    public <T> void performTest(
            StreamSource<T> source,
            FunctionEx<T, Boolean> mapFn
    ) {
        performTest(member, source, mapFn);
    }

    public <T> void performTest(
            HazelcastInstance producerInstance,
            StreamSource<T> streamSource,
            FunctionEx<T, Boolean> mapFn
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
        counter++;

        List<Boolean> sinkList = member.getList(sinkName);
        assertTrueEventually(() -> assertThat(sinkList.size()).isGreaterThan(0));

        job.suspend();
        JobAssertions.assertThat(job).eventuallyHasStatus(SUSPENDED);

        // Fill up the journal to cause lost events
        while (counter < 2 * JOURNAL_CAPACITY_PER_PARTITION) {
            put(producerInstance, 0, counter);
            counter++;
        }

        job.resume();
        JobAssertions.assertThat(job).eventuallyHasStatus(RUNNING);

        assertTrueEventually(() -> assertThat(sinkList.size()).isEqualTo(1 + JOURNAL_CAPACITY_PER_PARTITION));

        assertThat(sinkList).containsOnlyOnce(true);
    }

    protected abstract void put(HazelcastInstance instance, Integer key, Integer value);

}
