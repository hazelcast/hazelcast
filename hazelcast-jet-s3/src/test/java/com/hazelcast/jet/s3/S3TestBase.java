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

import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.test.Assertions;
import org.junit.Before;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static com.hazelcast.jet.pipeline.GenericPredicates.alwaysTrue;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static software.amazon.awssdk.core.sync.ResponseTransformer.toBytes;

abstract class S3TestBase extends JetTestSupport {

    JetInstance jet;

    @Before
    public void setupCluster() {
        jet = createJetMember();
        createJetMember();
    }

    void testSink(JetInstance jet, String bucketName) {
        IMapJet<Integer, String> map = jet.getMap("map");

        int itemCount = 20000;
        String prefix = "my-objects-";
        String payload = generateRandomString(1_000);

        for (int i = 0; i < itemCount; i++) {
            map.put(i, payload);
        }

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.map(map, alwaysTrue(), Map.Entry::getValue))
         .drainTo(S3Sinks.s3(bucketName, prefix, StandardCharsets.UTF_8, clientSupplier(), Object::toString));

        jet.newJob(p).join();

        try (S3Client client = clientSupplier().get()) {
            ListObjectsResponse listing = client.listObjects(req -> req.bucket(bucketName).prefix(prefix));
            long totalLineCount = listing.contents()
                                         .stream()
                                         .filter(object -> object.key().startsWith(prefix))
                                         .map(object -> client.getObject(req -> req.bucket(bucketName).key(object.key()),
                                                 toBytes()))
                                         .mapToLong(bytes -> assertPayloadAndCount(bytes.asByteArray(), payload))
                                         .sum();

            assertEquals(itemCount, totalLineCount);
        }
    }

    void testSource(JetInstance jet, String bucketName, String prefix, int objectCount, int lineCount) {
        Pipeline p = Pipeline.create();
        p.drawFrom(S3Sources.s3(singletonList(bucketName), prefix, clientSupplier()))
         .groupingKey(s -> s)
         .aggregate(AggregateOperations.counting())
         .apply(Assertions.assertCollected(entries -> {
             assertTrue(entries.stream().allMatch(
                     e -> e.getValue() == objectCount && e.getKey().matches("^line\\-\\d+$")
             ));
             assertEquals(lineCount, entries.size());
         }));

        jet.newJob(p).join();
    }

    abstract SupplierEx<S3Client> clientSupplier();

    static long assertPayloadAndCount(byte[] bytes, String expectedPayload) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bytes)))) {
            return reader.lines().peek(s -> assertEquals(expectedPayload, s)).count();
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }


}
