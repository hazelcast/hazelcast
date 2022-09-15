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

package com.hazelcast.jet.s3;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.s3.S3Sinks.S3SinkContext;
import com.hazelcast.jet.test.IgnoreInJenkinsOnWindows;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static java.lang.System.lineSeparator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static software.amazon.awssdk.core.sync.ResponseTransformer.toInputStream;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class, IgnoreInJenkinsOnWindows.class})
public class S3MockTest extends S3TestBase {

    private static S3MockContainer s3MockContainer;

    private static final ILogger logger = Logger.getLogger(S3MockTest.class);
    private static final String SOURCE_BUCKET = "source-bucket";
    private static final String SOURCE_BUCKET_2 = "source-bucket-2";
    private static final String SOURCE_BUCKET_EMPTY = "source-bucket-empty";
    private static final String SINK_BUCKET = "sink-bucket";
    private static final String SINK_BUCKET_OVERWRITE = "sink-bucket-overwrite";
    private static final String SINK_BUCKET_NONASCII = "sink-bucket-nonascii";

    private static final int LINE_COUNT = 100;

    private static S3Client s3Client;


    @BeforeClass
    public static void setupS3() {
        assumeDockerEnabled();
        s3MockContainer = new S3MockContainer();
        s3MockContainer.start();
        s3MockContainer.followOutput(outputFrame -> logger.info(outputFrame.getUtf8String().trim()));
        s3Client = s3MockContainer.client();
    }

    @AfterClass
    public static void teardown() {
        try {
            if (s3Client != null) {
                s3Client.close();
            }
        } finally {
            if (s3MockContainer != null) {
                s3MockContainer.stop();
            }
        }
    }

    @Before
    public void setup() {
        S3SinkContext.maximumPartNumber = 1;
        deleteBucket(s3Client, SOURCE_BUCKET);
        deleteBucket(s3Client, SOURCE_BUCKET_2);
        deleteBucket(s3Client, SOURCE_BUCKET_EMPTY);
        deleteBucket(s3Client, SINK_BUCKET);
        deleteBucket(s3Client, SINK_BUCKET_OVERWRITE);
        deleteBucket(s3Client, SINK_BUCKET_NONASCII);
    }

    @After
    public void resetPartNumber() {
        S3SinkContext.maximumPartNumber = S3SinkContext.DEFAULT_MAXIMUM_PART_NUMBER;
    }

    @Test
    public void when_manySmallItemsToSink() {
        s3Client.createBucket(b -> b.bucket(SINK_BUCKET));

        testSink(SINK_BUCKET, "many-small-items-", 1000);
    }

    @Test
    public void when_itemsToSinkIsLargerThanBuffer() {
        s3Client.createBucket(b -> b.bucket(SINK_BUCKET));

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < S3SinkContext.DEFAULT_MINIMUM_UPLOAD_PART_SIZE / 5; i++) {
            sb.append("01234567890");
        }
        String expected = sb.toString();

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(expected))
         .writeTo(S3Sinks.s3(SINK_BUCKET, null, UTF_8, clientSupplier(), Object::toString));

        hz.getJet().newJob(p).join();

        try (S3Client client = clientSupplier().get()) {
            List<String> lines = client
                    .listObjectsV2(req -> req.bucket(SINK_BUCKET))
                    .contents()
                    .stream()
                    .peek(o -> System.out.println("Found object: " + o))
                    .map(object -> client.getObject(req -> req.bucket(SINK_BUCKET).key(object.key()), toInputStream()))
                    .flatMap(this::inputStreamToLines)
                    .collect(Collectors.toList());

            assertEquals(1, lines.size());
            String actual = lines.get(0);
            assertEquals(expected.length(), actual.length());
            assertEquals(expected, actual);
        }
    }

    @Test
    public void when_simpleSource() {
        s3Client.createBucket(b -> b.bucket(SOURCE_BUCKET));
        generateAndUploadObjects(SOURCE_BUCKET, "object-", 20, LINE_COUNT);

        testSource(SOURCE_BUCKET, "object-", 20, LINE_COUNT);
    }

    @Test
    public void when_sourceWithPrefix() {
        s3Client.createBucket(b -> b.bucket(SOURCE_BUCKET));
        generateAndUploadObjects(SOURCE_BUCKET, "object-", 20, LINE_COUNT);

        testSource(SOURCE_BUCKET, "object-1", 11, LINE_COUNT);
    }

    @Test
    public void when_sourceReadFromFolder() {
        s3Client.createBucket(b -> b.bucket(SOURCE_BUCKET_2));
        generateAndUploadObjects(SOURCE_BUCKET_2, "object-", 4, LINE_COUNT);
        generateAndUploadObjects(SOURCE_BUCKET_2, "testFolder/object-", 5, LINE_COUNT);

        testSource(SOURCE_BUCKET_2, "testFolder", 5, LINE_COUNT);
    }

    @Test
    public void when_sourceWithNotExistingBucket() {
        testSourceWithNotExistingBucket("jet-s3-connector-test-bucket-source-THIS-BUCKET-DOES-NOT-EXIST");
    }

    @Test
    public void when_sourceWithNotExistingPrefix() {
        s3Client.createBucket(b -> b.bucket(SOURCE_BUCKET_2));
        generateAndUploadObjects(SOURCE_BUCKET_2, "object-", 4, LINE_COUNT);

        testSourceWithEmptyResults(SOURCE_BUCKET_2, "THIS-PREFIX-DOES-NOT-EXIST");
    }

    @Test
    public void when_sourceWithEmptyBucket() {
        s3Client.createBucket(b -> b.bucket(SOURCE_BUCKET_EMPTY));

        testSourceWithEmptyResults(SOURCE_BUCKET_EMPTY, null);
    }

    @Test
    public void when_sourceWithTwoBuckets() {
        s3Client.createBucket(b -> b.bucket(SOURCE_BUCKET));
        s3Client.createBucket(b -> b.bucket(SOURCE_BUCKET_2));
        generateAndUploadObjects(SOURCE_BUCKET, "object-", 20, LINE_COUNT);
        generateAndUploadObjects(SOURCE_BUCKET_2, "object-", 4, LINE_COUNT);
        generateAndUploadObjects(SOURCE_BUCKET_2, "testFolder/object-", 5, LINE_COUNT);

        List<String> buckets = new ArrayList<>();
        buckets.add(SOURCE_BUCKET);
        buckets.add(SOURCE_BUCKET_2);
        testSource(buckets, "object-3", 2, LINE_COUNT);
    }

    @Test
    public void when_sinkWritesToExistingFile_then_overwritesFile() {
        s3Client.createBucket(b -> b.bucket(SINK_BUCKET_OVERWRITE));
        testSink(SINK_BUCKET_OVERWRITE, "my-objects-", 100);
        testSink(SINK_BUCKET_OVERWRITE, "my-objects-", 200);
    }

    @Test
    public void when_writeToNotExistingBucket() {
        testSinkWithNotExistingBucket("jet-s3-connector-test-bucket-sink-THIS-BUCKET-DOES-NOT-EXIST");
    }

    @Test
    public void when_sourceWithNonAsciiSymbolInName() {
        s3Client.createBucket(b -> b.bucket(SOURCE_BUCKET_2));
        generateAndUploadObjects(SOURCE_BUCKET_2, "测试", 1, LINE_COUNT);

        testSource(SOURCE_BUCKET_2, "测试", 1, LINE_COUNT);
    }

    @Test
    public void when_sourceWithNonAsciiSymbolInFile() {
        s3Client.createBucket(b -> b.bucket(SOURCE_BUCKET_2));
        generateAndUploadObjects(SOURCE_BUCKET_2, "fileWithNonASCIISymbol", 1, LINE_COUNT, "测试-");

        testSource(SOURCE_BUCKET_2, "fileWithNonASCIISymbol", 1, LINE_COUNT, "^测试\\-\\d+$");
    }

    @Test
    public void when_sinkWithNonAsciiSymbolInName() {
        s3Client.createBucket(b -> b.bucket(SINK_BUCKET_NONASCII));

        testSink(SINK_BUCKET_NONASCII, "测试", 10);
    }

    @Test
    public void when_sinkWithNonAsciiSymbolInFile() {
        s3Client.createBucket(b -> b.bucket(SINK_BUCKET_NONASCII));

        testSink(SINK_BUCKET_NONASCII, "fileWithNonAsciiSymbol", 10, "测试");
    }

    SupplierEx<S3Client> clientSupplier() {
        return () -> S3MockContainer.client(s3MockContainer.endpointURL());
    }

    private void generateAndUploadObjects(String bucketName, String prefix, int objectCount, int lineCount) {
        generateAndUploadObjects(bucketName, prefix, objectCount, lineCount, "line-");
    }

    private void generateAndUploadObjects(String bucketName, String prefix, int objectCount, int lineCount,
            String textPrefix) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < objectCount; i++) {
            range(0, lineCount).forEach(j -> builder.append(textPrefix).append(j).append(lineSeparator()));
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(prefix + i)
                    .build();
            s3Client.putObject(putObjectRequest, RequestBody.fromString(builder.toString()));
            builder.setLength(0);
        }
    }
}
