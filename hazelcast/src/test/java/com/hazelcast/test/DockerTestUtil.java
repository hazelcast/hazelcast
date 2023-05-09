/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.test;

import com.hazelcast.test.jdbc.H2DatabaseProvider;
import com.hazelcast.test.jdbc.TestDatabaseProvider;

import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.Assume.assumeTrue;

public class DockerTestUtil {
    public static boolean dockerEnabled() {
        return !System.getProperties().containsKey("hazelcast.disable.docker.tests");
    }

    public static void assumeDockerEnabled() {
        assumeTrue(DockerTestUtil.dockerEnabled());
    }

    // Currently, Windows can not launch some providers
    public static void assumeTestDatabaseProviderCanLaunch(TestDatabaseProvider provider) {
        // If docker is not enabled
        if (!dockerEnabled()) {
            // Only in-memory providers can run
            assumeThat(provider)
                    .isInstanceOfAny(
                            H2DatabaseProvider.class
                    );
        }
    }
}
