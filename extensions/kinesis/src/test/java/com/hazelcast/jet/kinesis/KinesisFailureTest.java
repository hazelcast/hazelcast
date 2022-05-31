/*
 * Copyright 2021 Hazelcast Inc.
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

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.kinesis.impl.AwsConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.test.SerialTest;
import com.hazelcast.logging.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.ToxiproxyContainer.ContainerProxy;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.kinesis.KinesisSinks.MAXIMUM_KEY_LENGTH;
import static com.hazelcast.jet.kinesis.KinesisSinks.MAX_RECORD_SIZE;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static com.hazelcast.test.TestStringUtils.repeat;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.testcontainers.utility.DockerImageName.parse;

public class KinesisFailureTest extends AbstractKinesisTest {

    @ClassRule
    public static final Network NETWORK = Network.newNetwork();

    public static LocalStackContainer localStack;

    public static ToxiproxyContainer toxiProxy;

    private static AwsConfig AWS_CONFIG;
    private static AmazonKinesisAsync KINESIS;
    private static ContainerProxy PROXY;
    private static KinesisTestHelper HELPER;

    public KinesisFailureTest() {
        super(AWS_CONFIG, KINESIS, HELPER);
    }

    @BeforeClass
    public static void beforeClass() {
        assumeDockerEnabled();

        localStack = new LocalStackContainer(parse("localstack/localstack")
                .withTag("0.12.3"))
                .withNetwork(NETWORK)
                .withServices(Service.KINESIS);
        localStack.start();
        toxiProxy = new ToxiproxyContainer(parse("shopify/toxiproxy")
                .withTag("2.1.0"))
                .withNetwork(NETWORK)
                .withNetworkAliases("toxiproxy");
        toxiProxy.start();

        System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");
        // with the jackson versions we use (2.11.x) Localstack doesn't without disabling CBOR
        // https://github.com/localstack/localstack/issues/3208

        PROXY = toxiProxy.getProxy(localStack, 4566);

        AWS_CONFIG = new AwsConfig()
                .withEndpoint("http://" + PROXY.getContainerIpAddress() + ":" + PROXY.getProxyPort())
                .withRegion(localStack.getRegion())
                .withCredentials(localStack.getAccessKey(), localStack.getSecretKey());
        KINESIS = AWS_CONFIG.buildClient();
        HELPER = new KinesisTestHelper(KINESIS, STREAM, Logger.getLogger(KinesisIntegrationTest.class));
    }

    @AfterClass
    public static void afterClass() {
        if (KINESIS != null) {
            KINESIS.shutdown();
        }

        if (toxiProxy != null) {
            toxiProxy.stop();
        }
        if (localStack != null) {
            localStack.stop();
        }
    }

    @Test
    @Category(SerialTest.class)
    public void networkOutageWhenStarting() throws Exception {
        HELPER.createStream(10);

        System.err.println("Cutting network connection ...");
        PROXY.setConnectionCut(true);

        hz().getJet().newJob(getPipeline(kinesisSource().build()));
        Map<String, List<String>> expectedMessages = sendMessages();

        SECONDS.sleep(5);

        System.err.println("Network connection re-established");
        PROXY.setConnectionCut(false);

        assertMessages(expectedMessages, true, false);
    }

    @Test
    @Category(SerialTest.class)
    public void networkOutageWhileRunning() throws Exception {
        HELPER.createStream(10);

        hz().getJet().newJob(getPipeline(kinesisSource().build()));
        Map<String, List<String>> expectedMessages = sendMessages();

        //wait for some data to start coming out of the pipeline
        assertTrueEventually(() -> assertFalse(results.isEmpty()));

        System.err.println("Cutting network connection ...");
        PROXY.setConnectionCut(true);
        SECONDS.sleep(5);
        System.err.println("Network connection re-established");
        PROXY.setConnectionCut(false);

        assertMessages(expectedMessages, true, true);
        // duplication happens due AWS SDK internals (producer retries; details here:
        // https://docs.aws.amazon.com/streams/latest/dev/kinesis-record-processor-duplicates.html)
    }

    @Test
    @Category(SerialTest.class)
    @Ignore //AWS mock completely ignores the credentials passed to it, accepts anything (test passes on real backend)
    public void sinkWithIncorrectCredentials() {
        HELPER.createStream(1);

        AwsConfig awsConfig = new AwsConfig()
                .withCredentials("wrong_key", "wrong_key");
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(entry("k", new byte[0])))
                .writeTo(KinesisSinks.kinesis(STREAM)
                        .withEndpoint(awsConfig.getEndpoint())
                        .withRegion(awsConfig.getRegion())
                        .withCredentials(awsConfig.getAccessKey(), awsConfig.getSecretKey())
                        .withRetryStrategy(KinesisTestHelper.RETRY_STRATEGY)
                        .build());

        Job job = hz().getJet().newJob(p);
        assertThrowsJetException(job, "The security token included in the request is invalid");
    }

    @Test
    @Category(SerialTest.class)
    @Ignore //AWS mock completely ignores the credentials passed to it, accepts anything (test passes on real backend)
    public void sourceWithIncorrectCredentials() {
        HELPER.createStream(1);

        AwsConfig awsConfig = new AwsConfig()
                .withCredentials("wrong_key", "wrong_key");
        Pipeline p = Pipeline.create();
        p.readFrom(KinesisSources.kinesis(STREAM)
                    .withEndpoint(awsConfig.getEndpoint())
                    .withRegion(awsConfig.getRegion())
                    .withCredentials(awsConfig.getAccessKey(), awsConfig.getSecretKey())
                    .withRetryStrategy(KinesisTestHelper.RETRY_STRATEGY)
                    .build())
                .withoutTimestamps()
                .writeTo(Sinks.noop());

        Job job = hz().getJet().newJob(p);
        assertThrowsJetException(job, "The security token included in the request is invalid");
    }

    @Test
    @Category(SerialTest.class)
    public void keyTooShort() {
        Entry<String, byte[]> valid = entry("k", new byte[0]);
        Entry<String, byte[]> invalid = entry("", new byte[0]);
        invalidInputToSink(valid, invalid, "Key empty");
    }

    @Test
    @Category(SerialTest.class)
    public void keyTooLong() {
        Entry<String, byte[]> valid = entry(repeat("*", MAXIMUM_KEY_LENGTH), new byte[0]);
        Entry<String, byte[]> invalid = entry(repeat("*", MAXIMUM_KEY_LENGTH + 1), new byte[0]);
        invalidInputToSink(valid, invalid, "Key too long");
    }

    @Test
    @Category(SerialTest.class)
    public void valueTooLong() {
        Entry<String, byte[]> valid = entry("k", new byte[MAX_RECORD_SIZE - 1]);
        Entry<String, byte[]> invalid = entry("k", new byte[MAX_RECORD_SIZE]);
        invalidInputToSink(valid, invalid, "Encoded length (key + payload) is too big");
    }

    @Test
    @Category(SerialTest.class)
    public void valuePlusKeyTooLong() {
        Entry<String, byte[]> valid = entry("kkk", new byte[MAX_RECORD_SIZE - 3]);
        Entry<String, byte[]> invalid = entry("kkk", new byte[MAX_RECORD_SIZE - 2]);
        invalidInputToSink(valid, invalid, "Encoded length (key + payload) is too big");
    }

    private void invalidInputToSink(Entry<String, byte[]> valid, Entry<String, byte[]> invalid, String error) {
        HELPER.createStream(1);

        Job job1 = writeOneEntry(valid);
        job1.join();
        assertJobStatusEventually(job1, JobStatus.COMPLETED);

        Job job2 = writeOneEntry(invalid);
        assertThrowsJetException(job2, error);
    }

    private Job writeOneEntry(Entry<String, byte[]> entry) {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(entry))
                .writeTo(kinesisSink());

        return hz().getJet().newJob(p);
    }

    private static void assertThrowsJetException(Job job, String messageFragment) {
        try {
            job.getFuture().get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        } catch (ExecutionException ee) {
            //job completed exceptionally, as expected, we check the details of it
            assertThat(ee)
                    .hasCauseInstanceOf(JetException.class)
                    .hasMessageContaining(messageFragment);
        } catch (Throwable t) {
            throw sneakyThrow(t);
        }
    }

}
