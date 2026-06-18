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

package com.hazelcast.jet.files;

import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;

@QuickTest
@ParallelJVMTest
public class AWSHadoopShadingCrossCompatibilityTest {
    /**
     * Hadoop depends on AWS SDK, which shades Apache HTTP Client
     * <p>
     * We override Hadoop's AWS dependency version with our own, which leaves us vulnerable to conflicting versions.
     * Specifically, AWS SDK upgraded to the newer Apache HttpClient5 (included via shading), whereas Hadoop expects (and relies
     * on) the shaded HttpClient4. This is flagged by tests, but of such complexity that they are integration tests run on a
     * nightly basis rather than on a per-PR level - which could lead to AWS upgrades being initially merged/approved only to
     * fail later on. Hence this test exercises the potentially problematic area (Hadoop / AWS / HttpClient interactivity) in a
     * trivial manner such that it can be run on a per-PR level to validate if an AWS upgrade is safe.
     */
    @Test
    public void test() throws IOException {
        try (S3AFileSystem fs = new S3AFileSystem()) {
            fs.initialize(URI.create("s3a://dummy-bucket"), new Configuration());
        }
    }
}
