/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;
import java.util.stream.Collectors;

@Category(NightlyTest.class)
public class S3SinkTest extends S3TestBase {

    private static String bucketName = "jet-s3-connector-test-bucket-sink";

    @Before
    @After
    public void deleteObjects() {
        S3Client client = clientSupplier().get();
        List<ObjectIdentifier> identifiers = client
                .listObjects(ListObjectsRequest.builder().bucket(bucketName).build())
                .contents()
                .stream()
                .map(S3Object::key)
                .map(key -> ObjectIdentifier.builder().key(key).build())
                .collect(Collectors.toList());

        if (!identifiers.isEmpty()) {
            client.deleteObjects(b -> b.bucket(bucketName).delete(d -> d.objects(identifiers)));
        }
    }

    @Test
    public void test() {
        testSink(bucketName);
    }

    @Test
    public void when_writesToExistingFile_then_overwritesFile() {
        testSink(bucketName, "my-objects-", 100);
        testSink(bucketName, "my-objects-", 200);
    }

    @Test
    public void when_writeToNotExistingBucket() {
        testSinkWithNotExistingBucket("jet-s3-connector-test-bucket-sink-THIS-BUCKET-DOES-NOT-EXIST");
    }

    @Test
    public void when_withSpaceInName() {
        testSink(bucketName, "file with space", 10);
    }

    @Test
    public void when_withNonAsciiSymbolInName() {
        testSink(bucketName, "测试", 10);
    }

    @Test
    public void when_withNonAsciiSymbolInFile() {
        testSink(bucketName, "fileWithNonAsciiSymbol", 10, "测试");
    }

    SupplierEx<S3Client> clientSupplier() {
        return () -> S3Client
                .builder()
                .region(Region.US_EAST_1)
                .build();
    }

}
