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
package com.hazelcast.mapstore;

import com.hazelcast.test.jdbc.H2DatabaseProvider;
import org.junit.jupiter.api.BeforeAll;

import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;

public class H2GenericMapStoreIT extends GenericMapStoreIT {

    @BeforeAll
    public static void beforeClass() {
        assumeDockerEnabled();
        initializeBeforeClass(new H2DatabaseProvider());
    }
}
