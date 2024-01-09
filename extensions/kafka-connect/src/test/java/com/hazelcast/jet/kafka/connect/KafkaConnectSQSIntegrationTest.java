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
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import javax.annotation.Nonnull;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static com.hazelcast.test.OverridePropertyRule.set;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class KafkaConnectSQSIntegrationTest extends JetTestSupport {
    @ClassRule
    public static final OverridePropertyRule enableLogging = set("hazelcast.logging.type", "log4j2");

    @ClassRule
    public static LocalStackContainer container =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.0.2"))
            .withServices(LocalStackContainer.Service.SQS)
            .withEnv("SQS_ENDPOINT_STRATEGY", "path");

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectSQSIntegrationTest.class);

    private static final String QUEUE_NAME = "myqueue";

    private static final int ITEM_COUNT = 1_000;

    private String queueUrl;

    @BeforeClass
    public static void setUpDocker() {
        assumeDockerEnabled();
    }

    @Test
    public void testReadFromSQSConnector() throws Exception {
        insertData();

        Pipeline pipeline = Pipeline.create();

        Properties connectorProperties = getConnectorProperties();
        StreamSource<String> streamSource = KafkaConnectSources.connect(connectorProperties,
                SourceRecordUtil::convertToString);

        StreamStage<String> streamStage = pipeline.readFrom(streamSource)
                .withoutTimestamps()
                .setLocalParallelism(2);
        streamStage.writeTo(Sinks.logger());

        Sink<String> sink = AssertionSinks.assertCollectedEventually(60,
                list -> assertEquals(ITEM_COUNT, list.size()));
        streamStage.writeTo(sink);

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJar(getSQSConnectorURL());

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        LOGGER.info("Creating a job");
        Job job = createHazelcastInstance(config).getJet().newJob(pipeline, jobConfig);
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


    @Nonnull
    private Properties getConnectorProperties() {
        Properties connectorProperties = new Properties();
        connectorProperties.setProperty("name", "sqs-source");
        connectorProperties.setProperty("connector.class", "com.nordstrom.kafka.connect.sqs.SqsSourceConnector");
        connectorProperties.setProperty("tasks.max", "1");
        // Region specific URL. Returns the mapped port e.g. http://127.0.0.1:7871
        String endpointUrl = container.getEndpointOverride(LocalStackContainer.Service.SQS).toString();
        connectorProperties.setProperty("sqs.endpoint.url", endpointUrl);

        connectorProperties.setProperty("sqs.queue.url", queueUrl);
        connectorProperties.setProperty("topics", "test-sqs-source");

        return connectorProperties;
    }

    void insertData() throws URISyntaxException {
        System.setProperty("aws.accessKeyId", container.getAccessKey());
        System.setProperty("aws.secretKey", container.getSecretKey());

        SqsClient sqs = SqsClient.builder()
                .endpointOverride(container.getEndpointOverride(LocalStackContainer.Service.SQS))
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(container.getAccessKey(), container.getSecretKey())
                        )
                )
                .region(Region.of(container.getRegion()))
                .build();

        createQueue(sqs);
        sendBatchMessages(sqs);
    }

    private void createQueue(SqsClient sqsClient) throws URISyntaxException {
        LOGGER.info("Create Queue");
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(QUEUE_NAME)
                .build();

        sqsClient.createQueue(createQueueRequest);

        LOGGER.info("Get queue url");
        GetQueueUrlResponse getQueueUrlResponse = sqsClient.
                getQueueUrl(GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build());
        String originalUrl = getQueueUrlResponse.queueUrl();
        queueUrl = changePort(originalUrl);

        LOGGER.info("sqs.queue.url {} ", queueUrl);
    }

    @Nonnull
    private String changePort(String str) throws URISyntaxException {
        URI originalUri = new URI(str);

        // Replace port with a new port (e.g., 9090)
        int newPort = container.getFirstMappedPort();
        URI updatedUri = new URI(
                originalUri.getScheme(),
                originalUri.getUserInfo(),
                originalUri.getHost(),
                newPort,
                originalUri.getPath(),
                originalUri.getQuery(),
                originalUri.getFragment()
        );
        return updatedUri.toString();
    }

    private void sendBatchMessages(SqsClient sqsClient) {
        LOGGER.info("Send multiple messages");

        List<SendMessageBatchRequestEntry> messageList = messageList();

        // Size of each sublist
        int sublistSize = 10;

        // Iterate the list with sublists of size 10
        for (int i = 0; i < messageList.size(); i += sublistSize) {
            int endIndex = Math.min(i + sublistSize, messageList.size());
            List<SendMessageBatchRequestEntry> sublist = messageList.subList(i, endIndex);

            // Process the current sublist
            SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder()
                    .queueUrl(queueUrl)
                    .entries(sublist)
                    .build();
            sqsClient.sendMessageBatch(sendMessageBatchRequest);
        }
    }

    List<SendMessageBatchRequestEntry> messageList() {
        ArrayList<SendMessageBatchRequestEntry> list = new ArrayList<>();
        for (int index = 0; index < ITEM_COUNT; index++) {
            SendMessageBatchRequestEntry requestEntry = SendMessageBatchRequestEntry.builder()
                    .id("id" + index)
                    .messageBody("msg" + index)
                    .build();
            list.add(requestEntry);
        }
        return list;
    }

    private URL getSQSConnectorURL() throws URISyntaxException {
        ClassLoader classLoader = getClass().getClassLoader();
        final String CONNECTOR_FILE_PATH = "kafka-connect-sqs-1.5.0.jar";
        URL resource = classLoader.getResource(CONNECTOR_FILE_PATH);
        assert resource != null;
        assertThat(new File(resource.toURI())).exists();

        return resource;
    }
}
