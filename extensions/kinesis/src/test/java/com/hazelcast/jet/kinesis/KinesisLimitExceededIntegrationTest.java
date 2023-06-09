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

package com.hazelcast.jet.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.hazelcast.collection.IList;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.kinesis.impl.AwsConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;

import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.utility.DockerImageName.parse;

@Category(SlowTest.class)
public class KinesisLimitExceededIntegrationTest extends AbstractKinesisTest {

    public static LocalStackContainer localStack;

    private static AwsConfig AWS_CONFIG;
    private static AmazonKinesisAsync KINESIS;
    private static KinesisTestHelper HELPER;

    public KinesisLimitExceededIntegrationTest() {
        super(AWS_CONFIG, KINESIS, HELPER);
    }

    @BeforeClass
    public static void beforeClass() {
        // To run with real kinesis AWS credentials need be available
        // to be loaded by DefaultAWSCredentialsProviderChain.
        // Keep in mind the real Kinesis is paid service and once you
        // run it you should ensure that cleanup happened correctly.
        boolean useRealKinesis = Boolean.parseBoolean(System.getProperty("run.with.real.kinesis", "false"));

        if (useRealKinesis) {
            AWS_CONFIG = new AwsConfig()
                    .withEndpoint("https://kinesis.us-east-1.amazonaws.com")
                    .withRegion("us-east-1");
        } else {
            assumeDockerEnabled();

            localStack = new LocalStackContainer(parse("localstack/localstack")
                    .withTag(LOCALSTACK_VERSION))
                    // Introduce errors so some items fail to be written and need to be retried
                    .withEnv("KINESIS_ERROR_PROBABILITY", "0.25")
                    .withServices(Service.KINESIS);
            localStack.start();

            AWS_CONFIG = new AwsConfig()
                    .withEndpoint("http://" + localStack.getHost() + ":" + localStack.getMappedPort(4566))
                    .withRegion(localStack.getRegion())
                    .withCredentials(localStack.getAccessKey(), localStack.getSecretKey());
        }
        KINESIS = AWS_CONFIG.buildClient();
        HELPER = new KinesisTestHelper(KINESIS, STREAM);
    }

    @AfterClass
    public static void afterClass() {
        if (KINESIS != null) {
            KINESIS.shutdown();
        }

        if (localStack != null) {
            localStack.stop();
        }
    }

    /**
     * Regression test for https://github.com/hazelcast/hazelcast/issues/24774
     */
    @Test
    public void limitExceededForTerminalSnapshot() throws Exception {
        HELPER.createStream(1);

        StreamSource<Entry<String, byte[]>> generatorSource = SourceBuilder
                .stream("KinesisDataSource", procCtx -> new long[1])
                .<Entry<String, byte[]>>fillBufferFn((ctx, buf) -> {
                    long messageId = ctx[0];
                    byte[] data = Long.toString(messageId).getBytes(StandardCharsets.UTF_8);
                    buf.add(entry(String.valueOf(messageId), data));
                    ctx[0] += 1;
                    sleepMillis(10);
                })
                .createSnapshotFn(ctx -> ctx[0])
                .restoreSnapshotFn((ctx, state) -> ctx[0] = state.get(0))
                .build();

        Pipeline write = Pipeline.create();
        write.readFrom(generatorSource)
             .withoutTimestamps()
             .writeTo(kinesisSink().build());

        Job writeJob = hz().getJet().newJob(write,
                new JobConfig().setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE));

        Pipeline p = Pipeline.create();
        p.readFrom(kinesisSource().build())
         .withNativeTimestamps(0)
         .map(Entry::getKey)
         .peek()
         .writeTo(Sinks.list("result"));
        Job readJob = hz().getJet().newJob(p,
                new JobConfig().setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE));

        for (int i = 0; i < 10; i++) {
            // Make sure job is running before we suspend it
            assertJobStatusEventually(writeJob, JobStatus.RUNNING);

            logger.info("Suspend #" + i);

            writeJob.suspend();
            assertJobSuspendedEventually(writeJob);
            writeJob.resume();
            Thread.sleep(1000);
        }

        // Because of the limit exceeded the items are out of order,
        // if we canceled the job some items at the end would be missing
        writeJob.suspend();

        assertTrueEventually(() -> {
            IList<String> list = hz().getList("result");
            List<Integer> cleanList = list.stream().distinct().map(Integer::valueOf).sorted().collect(toList());
            Integer max = cleanList.stream().max(Integer::compareTo).orElseThrow();

            List<Integer> expected = IntStream.range(0, max + 1).boxed().collect(toList());

            assertThat(cleanList).containsAll(expected);
        }, 10);
    }

    public static <K, V> Entry<K, V> entry(K key, V value) {
        return new AbstractMap.SimpleImmutableEntry<>(key, value);
    }

}
