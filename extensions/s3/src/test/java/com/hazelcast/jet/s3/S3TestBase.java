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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.test.Assertions;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.map.IMap;
import org.junit.Before;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.query.Predicates.alwaysTrue;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static software.amazon.awssdk.core.sync.ResponseTransformer.toInputStream;

abstract class S3TestBase extends JetTestSupport {

    private static final Charset CHARSET = UTF_8;

    HazelcastInstance hz;

    @Before
    public void setupCluster() {
        hz = createHazelcastInstance();
        createHazelcastInstance();
    }

    void testSink(String bucketName, String prefix, int itemCount) {
        testSink(bucketName, prefix, itemCount, generateRandomString(100));
    }

    void testSink(String bucketName, String prefix, int itemCount, String payload) {
        IMap<Integer, String> map = hz.getMap("map");

        for (int i = 0; i < itemCount; i++) {
            map.put(i, payload);
        }

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.map(map, alwaysTrue(), Map.Entry::getValue))
         .writeTo(S3Sinks.s3(bucketName, prefix, CHARSET, clientSupplier(), Object::toString));

        hz.getJet().newJob(p).join();

        try (S3Client client = clientSupplier().get()) {
            assertTrueEventually(() -> {
                long lineCount = client
                        .listObjectsV2(req -> req.bucket(bucketName).prefix(prefix))
                        .contents()
                        .stream()
                        .flatMap(o -> s3ObjectToLines(o, client, bucketName))
                        .peek(line -> assertEquals(payload, line))
                        .count();
                assertEquals(itemCount, lineCount);
            });
        }
    }

    void testSource(String bucketName, String prefix, int objectCount, int lineCount) {
        testSource(bucketName, prefix, objectCount, lineCount, "^line\\-\\d+$");
    }

    void testSource(String bucketName, String prefix, int objectCount, int lineCount, String match) {
        testSource(singletonList(bucketName), prefix, objectCount, lineCount, match);
    }

    void testSource(List<String> bucketNames, String prefix, int objectCount, int lineCount) {
        testSource(bucketNames, prefix, objectCount, lineCount, "^line\\-\\d+$");
    }

    void testSource(List<String> bucketNames, String prefix, int objectCount, int lineCount, String match) {
        Pipeline p = Pipeline.create();
        p.readFrom(S3Sources.s3(bucketNames, prefix, clientSupplier()))
                .groupingKey(s -> s)
                .aggregate(AggregateOperations.counting())
                .apply(Assertions.assertCollected(entries -> {
                    assertTrue(entries.stream().allMatch(
                            e -> e.getValue() == objectCount && e.getKey().matches(match)
                    ));
                    assertEquals(lineCount, entries.size());
                }));

        hz.getJet().newJob(p).join();
    }

    public void testSourceWithEmptyResults(String bucketName, String prefix) {
        Pipeline p = Pipeline.create();
        p.readFrom(S3Sources.s3(singletonList(bucketName), prefix, clientSupplier()))
                .apply(Assertions.assertCollected(entries -> {
                    assertEquals(0, entries.size());
                }));

        hz.getJet().newJob(p).join();
    }

    public void testSourceWithNotExistingBucket(String bucketName) {
        Pipeline p = Pipeline.create();
        p.readFrom(S3Sources.s3(singletonList(bucketName), null, clientSupplier()))
                .writeTo(Sinks.logger());

        try {
            hz.getJet().newJob(p).join();
            fail();
        } catch (Exception e) {
            assertCausedByNoSuchBucketException(e);
        }
    }

    public void testSinkWithNotExistingBucket(String bucketName) {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items("item"))
                .writeTo(S3Sinks.s3(bucketName, "ignore", UTF_8, clientSupplier(), Object::toString));

        try {
            hz.getJet().newJob(p).join();
            fail();
        } catch (Exception e) {
            assertCausedByNoSuchBucketException(e);
        }
    }

    abstract SupplierEx<S3Client> clientSupplier();

    void deleteBucket(S3Client client, String bucket) {
        try {
            client.deleteBucket(b -> b.bucket(bucket));
        } catch (NoSuchBucketException ignored) {
        }
    }

    Stream<String> s3ObjectToLines(S3Object o, S3Client client, String bucketName) {
        try {
            ResponseInputStream<GetObjectResponse> is = client
                    .getObject(req -> req.bucket(bucketName).key(o.key()), toInputStream());
            return inputStreamToLines(is);
        } catch (S3Exception e) {
            logger.warning("S3 side is having eventual consistency issue that it could not" +
                    " find the key that is listed before. We ignore this issue.", e);
        }
        return Stream.empty();
    }

    Stream<String> inputStreamToLines(InputStream is) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, CHARSET))) {
            // materialize the stream, since we can't read it afterwards
            return reader.lines().collect(Collectors.toList()).stream();
        } catch (IOException e) {
            throw new AssertionError("Error reading file ", e);
        }
    }

    private void assertCausedByNoSuchBucketException(Exception e) {
        Throwable cause = e;
        while (cause != null) {
            if (cause instanceof NoSuchBucketException) {
                return;
            }
            cause = cause.getCause();
        }
        fail("Failure was not caused by NoSuchBucketException.");
    }

}
