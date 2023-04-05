/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.kafka.connect;

import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.kafka.connect.data.Values;
import org.jetbrains.annotations.NotNull;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.kafka.connect.KafkaConnectSources.connect;
import static com.hazelcast.test.OverridePropertyRule.set;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class KafkaConnectIntegrationTest extends JetTestSupport {
    @ClassRule
    public static final OverridePropertyRule enableLogging = set("hazelcast.logging.type", "log4j2");
    public static final int ITEM_COUNT = 1_000;

    private static final String CONNECTOR_URL = "https://repository.hazelcast.com/download/tests/"
            + "confluentinc-kafka-connect-datagen-0.6.0.zip";

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectIntegrationTest.class);

    @Test
    public void testReading() throws Exception {
        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "datagen-connector");
        randomProperties.setProperty("connector.class", "io.confluent.kafka.connect.datagen.DatagenConnector");
        randomProperties.setProperty("max.interval", "1");
        randomProperties.setProperty("kafka.topic", "users");
        randomProperties.setProperty("quickstart", "users");

        Pipeline pipeline = Pipeline.create();
        StreamStage<String> streamStage = pipeline.readFrom(connect(randomProperties))
                .withoutTimestamps()
                .setLocalParallelism(1)
                .map(record -> Values.convertToString(record.valueSchema(), record.value()));
        streamStage.writeTo(Sinks.logger());
        streamStage
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> assertEquals(ITEM_COUNT, list.size())));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(new URL(CONNECTOR_URL));

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        Job job = createHazelcastInstance(config).getJet().newJob(pipeline, jobConfig);

        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
            assertTrueEventually(() -> {
                List<Long> pollTotalList = getSourceRecordPollTotalList();
                assertThat(pollTotalList).isNotEmpty();
                Long sourceRecordPollTotal = pollTotalList.get(0);
                assertThat(sourceRecordPollTotal).isGreaterThan(ITEM_COUNT);
            });
        }
    }

    private static <T> List<T> getMBeanValues(ObjectName objectName, String attribute) throws Exception {
        return (List<T>) getMBeans(objectName).stream().map(i -> getAttribute(i, attribute)).collect(toList());
    }

    @Nonnull
    private static <T> T getAttribute(ObjectInstance objectInstance, String attribute) {
        try {
            return (T) ManagementFactory.getPlatformMBeanServer().getAttribute(objectInstance.getObjectName(), attribute);
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    private static List<ObjectInstance> getMBeans(ObjectName objectName) throws Exception {
        MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
        return new ArrayList<>(platformMBeanServer.queryMBeans(objectName, null));
    }

    @Test
    public void testScaling() throws Exception {
        int localParallelism = 3;
        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "datagen-connector");
        randomProperties.setProperty("connector.class", "io.confluent.kafka.connect.datagen.DatagenConnector");
        randomProperties.setProperty("max.interval", "1");
        randomProperties.setProperty("kafka.topic", "orders");
        randomProperties.setProperty("quickstart", "orders");

        Pipeline pipeline = Pipeline.create();
        StreamStage<Map.Entry<String, String>> streamStage = pipeline.readFrom(connect(randomProperties))
                .withoutTimestamps()
                .setLocalParallelism(localParallelism)
                .map(record -> entry(record.headers().lastWithName("task.id").value().toString(),
                        Values.convertToString(record.valueSchema(), record.value()))
                );
        streamStage
                .writeTo(AssertionSinks.assertCollectedEventually(120,
                        list -> {
                            Map<String, List<String>> recordsByTaskId = entriesToMap(list);
                            LOGGER.info("recordsByTaskId = " + countEntriesByTaskId(recordsByTaskId));
                            assertThat(recordsByTaskId).allSatisfy((taskId, records) ->
                                    assertThat(records.size()).isGreaterThan(ITEM_COUNT)
                            );
                        }));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(new URL(CONNECTOR_URL));

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        HazelcastInstance hazelcastInstance = createHazelcastInstances(config, 3)[0];
        Job job = hazelcastInstance.getJet().newJob(pipeline, jobConfig);

        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
            assertTrueEventually(() -> {
                List<Long> sourceRecordPollTotalList = getSourceRecordPollTotalList();
                assertThat(sourceRecordPollTotalList).hasSize(localParallelism);
                assertThat(sourceRecordPollTotalList).allSatisfy(a -> assertThat(a).isGreaterThan(ITEM_COUNT));
            });
            assertTrueEventually(() -> {
                List<Long> times = getSourceRecordPollTotalTimes();
                assertThat(times).hasSize(localParallelism);
                assertThat(times).allSatisfy(a -> assertThat(a).isNotNegative());
            });

        }
    }

    @NotNull
    private static List<Map.Entry<String, Integer>> countEntriesByTaskId(Map<String, List<String>> recordsByTaskId) {
        return recordsByTaskId.entrySet().stream().map(e -> entry(e.getKey(), e.getValue().size())).collect(toList());
    }

    @Nonnull
    private static Map<String, List<String>> entriesToMap(List<Map.Entry<String, String>> list) {
        return list.stream()
                .collect(Collectors.groupingBy(Map.Entry::getKey,
                        Collectors.mapping(Map.Entry::getValue, toList())));
    }

    private static List<Long> getSourceRecordPollTotalList() throws Exception {
        ObjectName objectName = new ObjectName("com.hazelcast:type=Metrics,prefix=kafka.connect,*");
        return getMBeanValues(objectName, "sourceRecordPollTotal");
    }


    private static List<Long> getSourceRecordPollTotalTimes() throws Exception {
        ObjectName objectName = new ObjectName("com.hazelcast:type=Metrics,prefix=kafka.connect,*");
        return getMBeanValues(objectName, "sourceRecordPollTotalAvgTime");
    }

    @Ignore // https://github.com/hazelcast/hazelcast/issues/24018
    @Test
    public void testSnapshotting() throws Exception {
        int localParallelism = 3;
        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "datagen-connector");
        randomProperties.setProperty("connector.class", "io.confluent.kafka.connect.datagen.DatagenConnector");
        randomProperties.setProperty("max.interval", "1");
        randomProperties.setProperty("kafka.topic", "users");
        randomProperties.setProperty("quickstart", "users");

        Pipeline pipeline = Pipeline.create();
        StreamStage<String> streamStage = pipeline.readFrom(connect(randomProperties))
                .withoutTimestamps()
                .setLocalParallelism(localParallelism)
                .map(record -> Values.convertToString(record.valueSchema(), record.value()) + ", task.id: "
                        + record.headers().lastWithName("task.id").value());
        streamStage.writeTo(Sinks.logger());
        streamStage
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> assertEquals(localParallelism * ITEM_COUNT, list.size())));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(new URL(CONNECTOR_URL));
        jobConfig.setProcessingGuarantee(AT_LEAST_ONCE);
        jobConfig.setSnapshotIntervalMillis(10);

        Config config = smallInstanceConfig();
        config.addMapConfig(new MapConfig("*")
                .setEventJournalConfig(new EventJournalConfig().setEnabled(true))
                .setBackupCount(3)
        );
        config.getJetConfig().setResourceUploadEnabled(true);
        HazelcastInstance[] hazelcastInstances = createHazelcastInstances(config, 3);
        JobRepository jobRepository = new JobRepository(hazelcastInstances[0]);
        Job job = hazelcastInstances[0].getJet().newJob(pipeline, jobConfig);
        assertJobStatusEventually(job, RUNNING);

        waitForFirstSnapshot(jobRepository, job.getId(), 30, false);

        hazelcastInstances[1].shutdown();

        assertTrueEventually(() -> assertEquals(2, hazelcastInstances[0].getCluster().getMembers().size()));

        assertJobStatusEventually(job, RUNNING);

        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }
    }

}
