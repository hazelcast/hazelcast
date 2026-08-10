/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.kafka.impl;

import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.impl.JobExecutionRecord;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Future;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class StreamKafkaPIT extends HazelcastTestSupport {

    private static final int INITIAL_PARTITION_COUNT = 4;

    private static KafkaTestSupport kafkaTestSupport;

    private static HazelcastInstance[] instances;

    private String topic1Name;
    private String topic2Name;

    @BeforeClass
    public static void beforeClass() throws IOException {
        kafkaTestSupport = KafkaTestSupport.create();
        kafkaTestSupport.createKafkaCluster();
    }

    @Before
    public void before() throws IOException {
        topic1Name = randomString();
        topic2Name = randomString();
        kafkaTestSupport.createTopic(topic1Name, INITIAL_PARTITION_COUNT);
        kafkaTestSupport.createTopic(topic2Name, INITIAL_PARTITION_COUNT);

        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.getJetConfig().setEnabled(true);
        instances = createHazelcastInstances(config, 2);
        waitAllForSafeState(instances);
    }

    @AfterClass
    public static void afterClass() {
        kafkaTestSupport.shutdownKafkaCluster();
    }

    @Test
    public void integrationTest_noSnapshotting() {
        integrationTest(ProcessingGuarantee.NONE);
    }

    @Test
    public void integrationTest_withSnapshotting() {
        integrationTest(EXACTLY_ONCE);
    }

    private void integrationTest(ProcessingGuarantee guarantee) {
        int messageCount = 20;
        String sinkListName = randomName();

        Pipeline p = Pipeline.create();
        p.readFrom(KafkaSources.kafka(properties(), topic1Name, topic2Name))
                .withoutTimestamps()
                .writeTo(Sinks.list(sinkListName));

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(guarantee);
        config.setSnapshotIntervalMillis(500);
        Job job = instances[0].getJet().newJob(p, config);
        assertThat(job).eventuallyHasStatus(RUNNING);

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            futures.add(kafkaTestSupport.produce(topic1Name, i, Integer.toString(i)));
            futures.add(kafkaTestSupport.produce(topic2Name, i - messageCount, Integer.toString(i - messageCount)));
        }
        waitForAll(futures);
        IList<Object> list = instances[0].getList(sinkListName);

        assertTrueEventually(() -> {
            assertEquals(messageCount * 2, list.size());
            for (int i = 0; i < messageCount; i++) {
                Entry<Integer, String> entry1 = createEntry(i);
                Entry<Integer, String> entry2 = createEntry(i - messageCount);
                assertTrue("missing entry: " + entry1, list.contains(entry1));
                assertTrue("missing entry: " + entry2, list.contains(entry2));
            }
        });

        if (guarantee != ProcessingGuarantee.NONE) {
            // wait until a new snapshot appears
            JobRepository jr = new JobRepository(instances[0]);
            long currentMax = jr.getJobExecutionRecord(job.getId()).snapshotId();
            assertTrueEventually(() -> {
                JobExecutionRecord jobExecutionRecord = jr.getJobExecutionRecord(job.getId());
                assertNotNull("jobExecutionRecord == null", jobExecutionRecord);
                long newMax = jobExecutionRecord.snapshotId();
                assertTrue("no snapshot produced", newMax > currentMax);
                System.out.println("snapshot " + newMax + " found, previous was " + currentMax);
            });

            // Bring down one member. Job should restart and drain additional items (and maybe
            // some of the previous duplicately).
            instances[1].getLifecycleService().terminate();

            futures.clear();
            for (int i = messageCount; i < 2 * messageCount; i++) {
                futures.add(kafkaTestSupport.produce(topic1Name, i, Integer.toString(i)));
                futures.add(kafkaTestSupport.produce(topic2Name, i - messageCount, Integer.toString(i - messageCount)));
            }
            waitForAll(futures);

            assertTrueEventually(() -> {
                assertTrue("Not all messages were received", list.size() >= messageCount * 4);
                for (int i = 0; i < 2 * messageCount; i++) {
                    Entry<Integer, String> entry1 = createEntry(i);
                    Entry<Integer, String> entry2 = createEntry(i - messageCount);
                    assertTrue("missing entry: " + entry1, list.contains(entry1));
                    assertTrue("missing entry: " + entry2, list.contains(entry2));
                }
            });
        }

        assertFalse(job.getFuture().isDone());

        // cancel the job
        job.cancel();
        assertTrueEventually(() -> assertTrue(job.getFuture().isDone()));
    }

    public static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaTestSupport.getBrokerConnectionString());
        properties.setProperty("key.deserializer", IntegerDeserializer.class.getCanonicalName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getCanonicalName());
        properties.setProperty("auto.offset.reset", "earliest");
        return properties;
    }

    private static Entry<Integer, String> createEntry(int i) {
        return new SimpleImmutableEntry<>(i, Integer.toString(i));
    }
}
