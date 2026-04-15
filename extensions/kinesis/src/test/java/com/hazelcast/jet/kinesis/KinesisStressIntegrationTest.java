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

package com.hazelcast.jet.kinesis;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.impl.JobProxy;
import com.hazelcast.jet.kinesis.impl.AwsConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.map.IMap;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.jet.TestedVersions.LOCALSTACK_IMAGE;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static com.hazelcast.test.HazelcastTestSupport.sleepMillis;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Test associated with <a href="https://hazelcast.atlassian.net/browse/CTT-661">CTT-661</a>, making sure that not closing the client won't hurt the cluster.
 */
@NightlyTest
@Timeout(value = 3, unit = HOURS)
public class KinesisStressIntegrationTest {
    private static final String STREAM = "KinesisStressIntegrationTestTestStream";

    private LocalStackContainer localStack;

    private KinesisAsyncClient kinesis;
    private AwsConfig awsConfig;
    private HazelcastInstance[] cluster;
    private IMap<String, List<String>> results;

    private final TestHazelcastInstanceFactory instanceFactory = new TestHazelcastInstanceFactory(3);
    private final SenderThread sender = new SenderThread();

    @BeforeEach
    public void before() {
        // To run with real kinesis AWS credentials need be available
        // to be loaded by DefaultAWSCredentialsProviderChain.
        // Keep in mind the real Kinesis is paid service and once you
        // run it you should ensure that cleanup happened correctly.
        boolean useRealKinesis = Boolean.parseBoolean(System.getProperty("run.with.real.kinesis", "false"));

        if (useRealKinesis) {
            awsConfig = new AwsConfig()
                    .withEndpoint("https://kinesis.us-east-1.amazonaws.com")
                    .withRegion("us-east-1");
        } else {
            assumeDockerEnabled();

            localStack = new LocalStackContainer(LOCALSTACK_IMAGE)
                    .withServices(Service.KINESIS);
            localStack.start();

            awsConfig = new AwsConfig()
                    .withEndpoint("http://" + localStack.getHost() + ":" + localStack.getMappedPort(4566))
                    .withRegion(localStack.getRegion())
                    .withCredentials(localStack.getAccessKey(), localStack.getSecretKey());
        }
        kinesis = awsConfig.buildClient();
        KinesisTestHelper helper = new KinesisTestHelper(kinesis, STREAM);
        helper.createStream(3);

        cluster = instanceFactory.newInstances(smallInstanceConfig());
        results = cluster[0].getMap(randomName());
    }

    @AfterEach
    public void after() {
        instanceFactory.terminateAll();

        sender.killHimKillHimNow();

        closeResource(kinesis);
        closeResource(localStack);
    }

    private static Stream<Arguments> testArguments() {
        return Stream.of(arguments(named("graceful restart", true)),
                         arguments(named("forceful restart", false)));
    }

    @ParameterizedTest
    @MethodSource("testArguments")
    @Timeout(value = 1, unit = HOURS)
    public void stressTest(boolean graceful) {
        final HazelcastInstance hz = cluster[0];

        Pipeline pipeline = createPipeline();

        JobConfig jobConfig = new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE)
                .setSnapshotIntervalMillis(SECONDS.toMillis(1));
        Job job = hz.getJet().newJob(pipeline, jobConfig);
        sender.start();

        //wait for some data to start coming out of the pipeline
        assertTrueEventually(() -> assertFalse(results.isEmpty()));

        int lastSeenCount = 0;
        for (int i = 0; i < 350; i++) {
            final int finalLastSeenCount = lastSeenCount;

            sleepSeconds(7);
            assertTrueEventually(() -> assertThat(sender.count()).isGreaterThanOrEqualTo(finalLastSeenCount));

            ((JobProxy) job).restart(graceful);

            lastSeenCount = results.size();
        }
    }

    private Pipeline createPipeline() {
        var source = KinesisSources.kinesis(STREAM)
                                   .withEndpoint(awsConfig.getEndpoint())
                                   .withRegion(awsConfig.getRegion())
                                   .withCredentials(awsConfig.getAccessKey(), awsConfig.getSecretKey())
                                   .build();
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withNativeTimestamps(0)
                .rebalance(Entry::getKey)
                .map(e -> entry(e.getKey(), List.of(new String(e.getValue()))))
                .writeTo(Sinks.mapWithMerging(results, Entry::getKey, Entry::getValue, (l1, l2) -> {
                    ArrayList<String> list = new ArrayList<>();
                    list.addAll(l1);
                    list.addAll(l2);
                    return list;
                }));
        return pipeline;
    }

    private class SenderThread extends Thread {
        private final AtomicInteger counter = new AtomicInteger();
        private volatile boolean produceItems = true;

        @Override
        public void run() {
            while (produceItems) {
                var value = counter.incrementAndGet();
                var request = PutRecordRequest.builder()
                                              .streamName(STREAM)
                                              .data(SdkBytes.fromString(String.valueOf(value), UTF_8))
                                              .partitionKey(String.valueOf(value % 100));
                kinesis.putRecord(request.build());

                sleepMillis(500);
            }
        }

        // good, goooood, let the hate flow through you...
        void killHimKillHimNow() {
            produceItems = false;
        }
        int count() {
            return counter.get();
        }
    }
}
