/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.s3;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.test.Assertions;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.map.IMap;
import com.hazelcast.function.SupplierEx;
import org.junit.Before;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

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

    JetInstance jet;

    @Before
    public void setupCluster() {
        jet = createJetMember();
        createJetMember();
    }

    void testSink(String bucketName) {
        testSink(bucketName, "my-objects-", 20000);
    }

    void testSink(String bucketName, String prefix, int itemCount) {
        testSink(bucketName, prefix, itemCount, generateRandomString(1_000));
    }

    void testSink(String bucketName, String prefix, int itemCount, String payload) {
        IMap<Integer, String> map = jet.getMap("map");

        for (int i = 0; i < itemCount; i++) {
            map.put(i, payload);
        }

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.map(map, alwaysTrue(), Map.Entry::getValue))
         .drainTo(S3Sinks.s3(bucketName, prefix, CHARSET, clientSupplier(), Object::toString));

        jet.newJob(p).join();

        try (S3Client client = clientSupplier().get()) {
            long lineCount = client
                    .listObjects(req -> req.bucket(bucketName).prefix(prefix))
                    .contents()
                    .stream()
                    .map(o -> client.getObject(req -> req.bucket(bucketName).key(o.key()), toInputStream()))
                    .flatMap(this::inputStreamToLines)
                    .peek(line -> assertEquals(payload, line))
                    .count();
            assertEquals(itemCount, lineCount);
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
        p.drawFrom(S3Sources.s3(bucketNames, prefix, clientSupplier()))
                .groupingKey(s -> s)
                .aggregate(AggregateOperations.counting())
                .apply(Assertions.assertCollected(entries -> {
                    assertTrue(entries.stream().allMatch(
                            e -> e.getValue() == objectCount && e.getKey().matches(match)
                    ));
                    assertEquals(lineCount, entries.size());
                }));

        jet.newJob(p).join();
    }

    public void testSourceWithEmptyResults(String bucketName, String prefix) {
        Pipeline p = Pipeline.create();
        p.drawFrom(S3Sources.s3(singletonList(bucketName), prefix, clientSupplier()))
                .apply(Assertions.assertCollected(entries -> {
                    assertEquals(0, entries.size());
                }));

        jet.newJob(p).join();
    }

    public void testSourceWithNotExistingBucket(String bucketName) {
        Pipeline p = Pipeline.create();
        p.drawFrom(S3Sources.s3(singletonList(bucketName), null, clientSupplier()))
                .drainTo(Sinks.logger());

        try {
            jet.newJob(p).join();
            fail();
        } catch (Exception e) {
            assertCausedByNoSuchBucketException(e);
        }
    }

    public void testSinkWithNotExistingBucket(String bucketName) {
        Pipeline p = Pipeline.create();
        p.drawFrom(TestSources.items("item"))
                .drainTo(S3Sinks.s3(bucketName, "ignore", UTF_8, clientSupplier(), Object::toString));

        try {
            jet.newJob(p).join();
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
