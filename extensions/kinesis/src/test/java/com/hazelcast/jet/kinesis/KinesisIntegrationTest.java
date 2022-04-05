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
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.JobProxy;
import com.hazelcast.jet.kinesis.impl.AwsConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.test.SerialTest;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static com.amazonaws.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.AT_SEQUENCE_NUMBER;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.AT_TIMESTAMP;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.LATEST;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.TRIM_HORIZON;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.pipeline.test.Assertions.assertCollectedEventually;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.testcontainers.utility.DockerImageName.parse;

public class KinesisIntegrationTest extends AbstractKinesisTest {

    public static LocalStackContainer localStack;

    private static AwsConfig AWS_CONFIG;
    private static AmazonKinesisAsync KINESIS;
    private static KinesisTestHelper HELPER;
    private static boolean useRealKinesis;

    public KinesisIntegrationTest() {
        super(AWS_CONFIG, KINESIS, HELPER);
    }

    @BeforeClass
    public static void beforeClass() {
        assumeDockerEnabled();

        localStack = new LocalStackContainer(parse("localstack/localstack")
                .withTag("0.12.3"))
                .withServices(Service.KINESIS);
        localStack.start();

        // To run with real kinesis AWS credentials need be available
        // to be loaded by DefaultAWSCredentialsProviderChain.
        // Keep in mind the real Kinesis is paid service and once you
        // run it you should ensure that cleanup happened correctly.
        useRealKinesis = Boolean.parseBoolean(System.getProperty("run.with.real.kinesis", "false"));

        System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");
        // with the jackson versions we use (2.11.x) Localstack doesn't without disabling CBOR
        // https://github.com/localstack/localstack/issues/3208

        if (useRealKinesis) {
            AWS_CONFIG = new AwsConfig()
                    .withEndpoint("https://kinesis.us-east-1.amazonaws.com")
                    .withRegion("us-east-1");
        } else {
            AWS_CONFIG = new AwsConfig()
                    .withEndpoint("http://" + localStack.getHost() + ":" + localStack.getMappedPort(4566))
                    .withRegion(localStack.getRegion())
                    .withCredentials(localStack.getAccessKey(), localStack.getSecretKey());
        }
        KINESIS = AWS_CONFIG.buildClient();
        HELPER = new KinesisTestHelper(KINESIS, STREAM, Logger.getLogger(KinesisIntegrationTest.class));
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

    @Test
    @Category(SerialTest.class)
    public void timestampsAndWatermarks() {
        HELPER.createStream(1);

        sendMessages();

        try {
            Pipeline pipeline = Pipeline.create();
            pipeline.readFrom(kinesisSource().build())
                    .withNativeTimestamps(0)
                    .window(WindowDefinition.sliding(500, 100))
                    .aggregate(counting())
                    .apply(assertCollectedEventually(ASSERT_TRUE_EVENTUALLY_TIMEOUT, windowResults -> {
                        assertTrue(windowResults.size() > 1); //multiple windows, so watermark works
                    }));

            hz().getJet().newJob(pipeline).join();
            fail("Expected exception not thrown");
        } catch (CompletionException ce) {
            Throwable cause = peel(ce);
            assertTrue(cause instanceof JetException);
            assertTrue(cause.getCause() instanceof AssertionCompletedException);
        }
    }

    @Test
    @Category(SerialTest.class)
    public void customProjection() {
        HELPER.createStream(1);

        sendMessages();
        Long expectedPerSequenceNo = 1L;

        try {
            Pipeline pipeline = Pipeline.create();
            StreamSource<String> source = kinesisSource()
                    .withProjectionFn((r, s) -> {
                        byte[] payload = new byte[r.getData().remaining()];
                        r.getData().get(payload);
                        return r.getSequenceNumber();
                    })
                    .build();
            pipeline.readFrom(source)
                    .withoutTimestamps()
                    .groupingKey(r -> r)
                    .rollingAggregate(counting())
                    .apply(assertCollectedEventually(ASSERT_TRUE_EVENTUALLY_TIMEOUT, results -> {
                        assertEquals(MESSAGES, results.size());
                        results.forEach(v -> assertEquals(expectedPerSequenceNo, v.getValue()));
                    }));

            hz().getJet().newJob(pipeline).join();
            fail("Expected exception not thrown");
        } catch (CompletionException ce) {
            Throwable cause = peel(ce);
            assertTrue(cause instanceof JetException);
            assertTrue(cause.getCause() instanceof AssertionCompletedException);
        }
    }

    @Test
    @Category(SerialTest.class)
    public void staticStream_1Shard() {
        staticStream(1);
    }

    @Test
    @Category({SerialTest.class, NightlyTest.class})
    public void staticStream_2Shards() {
        staticStream(2);
    }

    @Test
    @Category({SerialTest.class, NightlyTest.class})
    public void staticStream_50Shards() {
        staticStream(50);
    }

    private void staticStream(int shards) {
        HELPER.createStream(shards);

        hz().getJet().newJob(getPipeline(kinesisSource().build()));

        Map<String, List<String>> expectedMessages = sendMessages();
        assertMessages(expectedMessages, true, false);
    }

    @Test
    @Category({SerialTest.class, NightlyTest.class})
    public void dynamicStream_2Shards_mergeBeforeData() {
        HELPER.createStream(2);

        List<Shard> shards = listOpenShards();
        Shard shard1 = shards.get(0);
        Shard shard2 = shards.get(1);

        mergeShards(shard1, shard2);
        HELPER.waitForStreamToActivate();
        assertOpenShards(1, shard1, shard2);

        hz().getJet().newJob(getPipeline(kinesisSource().build()));

        Map<String, List<String>> expectedMessages = sendMessages();
        assertMessages(expectedMessages, true, false);
    }

    @Test
    @Category(SerialTest.class)
    public void dynamicStream_2Shards_mergeDuringData() {
        dynamicStream_mergesDuringData(2, 1);
    }

    @Test
    @Category({SerialTest.class, NightlyTest.class})
    public void dynamicStream_50Shards_mergesDuringData() {
        //important to test with more shards than can fit in a single list shards response
        dynamicStream_mergesDuringData(50, 5);
    }

    private void dynamicStream_mergesDuringData(int shards, int merges) {
        HELPER.createStream(shards);

        hz().getJet().newJob(getPipeline(kinesisSource().build()));

        Map<String, List<String>> expectedMessages = sendMessages();

        //wait for some data to start coming out of the pipeline, before starting the merging
        assertTrueEventually(() -> assertFalse(results.isEmpty()));

        for (int i = 0; i < merges; i++) {
            List<Shard> openShards = listOpenShards();
            Collections.shuffle(openShards);

            Tuple2<Shard, Shard> adjacentPair = findAdjacentPair(openShards.get(0), openShards);
            Shard shard1 = adjacentPair.f0();
            Shard shard2 = adjacentPair.f1();

            mergeShards(shard1, shard2);
            HELPER.waitForStreamToActivate();
            assertOpenShards(shards - i - 1, shard1, shard2);
        }

        assertMessages(expectedMessages, false, false);
    }

    @Test
    @Category({SerialTest.class, NightlyTest.class})
    public void dynamicStream_1Shard_splitBeforeData() {
        HELPER.createStream(1);

        Shard shard = listOpenShards().get(0);

        splitShard(shard);
        HELPER.waitForStreamToActivate();
        assertOpenShards(2, shard);

        hz().getJet().newJob(getPipeline(kinesisSource().build()));

        Map<String, List<String>> expectedMessages = sendMessages();
        assertMessages(expectedMessages, true, false);
    }

    @Test
    @Category(SerialTest.class)
    public void dynamicStream_1Shard_splitsDuringData() {
        dynamicStream_splitsDuringData(1, 3);
    }

    @Test
    @Category({SerialTest.class, NightlyTest.class})
    public void dynamicStream_10Shards_splitsDuringData() {
        dynamicStream_splitsDuringData(10, 10);
    }

    private void dynamicStream_splitsDuringData(int shards, int splits) {
        HELPER.createStream(shards);

        hz().getJet().newJob(getPipeline(kinesisSource().build()));

        Map<String, List<String>> expectedMessages = sendMessages();

        //wait for some data to start coming out of the pipeline, before starting the splits
        assertTrueEventually(() -> assertFalse(results.isEmpty()));

        List<Shard> openShards;
        for (int i = 0; i < splits; i++) {
            openShards = listOpenShards();
            Collections.shuffle(openShards);
            Shard shard = openShards.get(0);

            splitShard(shard);
            HELPER.waitForStreamToActivate();
            assertOpenShards(openShards.size() + 1, shard);
        }

        assertMessages(expectedMessages, false, false);
    }

    @Test
    @Category(SerialTest.class)
    public void restart_staticStream_graceful() {
        restart_staticStream(true);
    }

    @Test
    @Category(SerialTest.class)
    public void restart_staticStream_non_graceful() {
        restart_staticStream(false);
    }

    private void restart_staticStream(boolean graceful) {
        HELPER.createStream(3);

        JobConfig jobConfig = new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE)
                .setSnapshotIntervalMillis(SECONDS.toMillis(1));
        Job job = hz().getJet().newJob(getPipeline(kinesisSource().build()), jobConfig);

        Map<String, List<String>> expectedMessages = sendMessages();

        //wait for some data to start coming out of the pipeline
        assertTrueEventually(() -> assertFalse(results.isEmpty()));

        ((JobProxy) job).restart(graceful);

        assertMessages(expectedMessages, true, !graceful);
    }

    @Test
    @Category(SerialTest.class)
    public void restart_dynamicStream_graceful() {
        restart_dynamicStream(true);
    }

    @Test
    @Category({SerialTest.class, NightlyTest.class})
    public void restart_dynamicStream_non_graceful() {
        restart_dynamicStream(false);
    }

    private void restart_dynamicStream(boolean graceful) {
        HELPER.createStream(3);

        JobConfig jobConfig = new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE)
                .setSnapshotIntervalMillis(SECONDS.toMillis(1));
        Job job = hz().getJet().newJob(getPipeline(kinesisSource().build()), jobConfig);

        Map<String, List<String>> expectedMessages = sendMessages();

        //wait for some data to start coming out of the pipeline
        assertTrueEventually(() -> assertFalse(results.isEmpty()));

        List<Shard> openShards = listOpenShards();

        Shard shard1 = openShards.get(0);
        Shard shard2 = openShards.get(1);
        Shard shard3 = openShards.get(2);

        splitShard(shard1);
        HELPER.waitForStreamToActivate();
        assertOpenShards(4, shard1);

        mergeShards(shard2, shard3);
        HELPER.waitForStreamToActivate();
        assertOpenShards(3, shard2, shard3);

        ((JobProxy) job).restart(graceful);

        assertMessages(expectedMessages, false, !graceful);
    }

    @Test
    @Category(SerialTest.class)
    public void jobsStartedBeforeStreamExists() {
        Map<String, List<String>> expectedMessages = sendMessages(100);
        Job job = hz().getJet().newJob(getPipeline(kinesisSource().build()));
        assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()));

        HELPER.createStream(1);

        assertMessages(expectedMessages, true, false);
    }

    @Test
    @Ignore
    //
    // Kinesis seems to mess up the timestamps internally somehow, don't know the exact reason...
    //
    // The error we get is: The timestampInMillis parameter cannot be greater than the currentTimestampInMillis.
    // timestampInMillis: 1610448760259000, currentTimestampInMillis: 1610448760774
    // (Service: AmazonKinesis; Status Code: 400; Error Code: InvalidArgumentException
    //
    // We seem to be sending the right request, not sure what causes the problem.
    //
    // Happens with latest AWS SDK too.
    //
    public void initialRead_timestamp() {
        HELPER.createStream(1);

        //send out some records, make sure they are in the shard
        HELPER.putRecords(messages(0, 100));
        Job initialJob = hz().getJet().newJob(getPipeline(kinesisSource().build()));
        assertMessages(expectedMessages(0, 100), true, false);
        initialJob.cancel();
        results.clear();

        //mark the time and send out more records
        long timestamp = System.currentTimeMillis();
        HELPER.putRecords(messages(100, 200));

        //start a job reading only records after the timestamp
        StreamSource<Map.Entry<String, byte[]>> source = kinesisSource()
                .withInitialShardIteratorRule(".*", AT_TIMESTAMP.name(), Long.toString(timestamp))
                .build();
        Job job = hz().getJet().newJob(getPipeline(source));
        assertJobStatusEventually(job, JobStatus.RUNNING);

        //check job has read only records from after the marked time
        assertMessages(expectedMessages(100, 200), true, false);
    }

    @Test
    public void initialRead_latest() throws Exception {
        HELPER.createStream(1);

        //send out some records, make sure they are in the shard
        HELPER.putRecords(messages(0, 100));
        Job initialJob = hz().getJet().newJob(getPipeline(kinesisSource().build()));
        assertMessages(expectedMessages(0, 100), true, false);
        initialJob.cancel();
        results.clear();

        //start a new job reading only latest records
        StreamSource<Map.Entry<String, byte[]>> source = kinesisSource()
                .withInitialShardIteratorRule(".*", LATEST.name(), null)
                .build();
        Job job = hz().getJet().newJob(getPipeline(source));
        assertJobStatusEventually(job, JobStatus.RUNNING);

        // need to be sure that reading the shard has commenced ...
        SECONDS.sleep(3);

        //send some more messages and check that the job only reads those
        HELPER.putRecords(messages(100, 200));
        assertMessages(expectedMessages(100, 200), true, false);
    }

    @Test
    public void initialRead_oldest() {
        HELPER.createStream(1);

        //send out some records, make sure they are in the shard
        HELPER.putRecords(messages(0, 100));
        Job initialJob = hz().getJet().newJob(getPipeline(kinesisSource().build()));
        assertMessages(expectedMessages(0, 100), true, false);
        initialJob.cancel();
        results.clear();

        //start a new job which reads all records
        StreamSource<Map.Entry<String, byte[]>> source = kinesisSource()
                .withInitialShardIteratorRule(".*", TRIM_HORIZON.name(), null)
                .build();
        Job job = hz().getJet().newJob(getPipeline(source));
        assertJobStatusEventually(job, JobStatus.RUNNING);

        //send some more messages and check that the job reads both old and new records
        HELPER.putRecords(messages(100, 200));
        assertMessages(expectedMessages(0, 200), true, false);
    }

    @Test
    public void initialRead_default() {
        HELPER.createStream(1);

        //send out some records, make sure they are in the shard
        HELPER.putRecords(messages(0, 100));
        Job initialJob = hz().getJet().newJob(getPipeline(kinesisSource().build()));
        assertMessages(expectedMessages(0, 100), true, false);
        initialJob.cancel();
        results.clear();

        //start a new job which reads records in its default way
        StreamSource<Map.Entry<String, byte[]>> source = kinesisSource().build();
        Job job = hz().getJet().newJob(getPipeline(source));
        assertJobStatusEventually(job, JobStatus.RUNNING);

        //send some more messages and check that the job reads both old and new records
        HELPER.putRecords(messages(100, 200));
        assertMessages(expectedMessages(0, 200), true, false);
    }

    @Test
    public void initialRead_atSequenceNumber() {
        HELPER.createStream(1);

        //send out some records, make sure they are in the shard
        PutRecordsResult putRecordsResult = HELPER.putRecords(messages(0, 100));
        Job initialJob = hz().getJet().newJob(getPipeline(kinesisSource().build()));
        assertMessages(expectedMessages(0, 100), true, false);
        initialJob.cancel();
        results.clear();

        String sequenceNumber = putRecordsResult.getRecords().get(50).getSequenceNumber();

        //start a new job which reads records from the sequence nomber (inclusive)
        StreamSource<Map.Entry<String, byte[]>> source = kinesisSource()
                .withInitialShardIteratorRule(".*", AT_SEQUENCE_NUMBER.name(), sequenceNumber)
                .build();
        hz().getJet().newJob(getPipeline(source));
        assertMessages(expectedMessages(50, 100), true, false);
    }

    @Test
    public void initialRead_afterSequenceNumber() {
        HELPER.createStream(1);

        //send out some records, make sure they are in the shard
        PutRecordsResult putRecordsResult = HELPER.putRecords(messages(0, 100));
        Job initialJob = hz().getJet().newJob(getPipeline(kinesisSource().build()));
        assertMessages(expectedMessages(0, 100), true, false);
        initialJob.cancel();
        results.clear();

        String sequenceNumber = putRecordsResult.getRecords().get(50).getSequenceNumber();

        //start a new job which reads records from the sequence nomber (inclusive)
        StreamSource<Map.Entry<String, byte[]>> source = kinesisSource()
                .withInitialShardIteratorRule(".*", AFTER_SEQUENCE_NUMBER.name(), sequenceNumber)
                .build();
        hz().getJet().newJob(getPipeline(source));
        assertMessages(expectedMessages(51, 100), true, false);
    }

    private void assertOpenShards(int count, Shard... excludedShards) {
        assertTrueEventually(() -> {
            Set<String> openShards = listOpenShards().stream().map(Shard::getShardId).collect(Collectors.toSet());
            assertEquals(count, openShards.size());
            for (Shard excludedShard : excludedShards) {
                assertFalse(openShards.contains(excludedShard.getShardId()));
            }
        });

    }

    @Override
    protected void assertMessages(Map<String, List<String>> expected, boolean checkOrder, boolean deduplicate) {
        if (checkOrder && useRealKinesis) {
            logger.info("Order cannot be checked on real Kinesis due to send/read limits. Assert will run without "
                    + "checking order.");
            checkOrder = false;
        }
        super.assertMessages(expected, checkOrder, deduplicate);
    }

}
