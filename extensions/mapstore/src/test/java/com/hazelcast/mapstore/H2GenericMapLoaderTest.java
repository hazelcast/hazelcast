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

import com.hazelcast.jet.test.SerialTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import org.junit.jupiter.api.BeforeAll;

/**
 * This test runs the MapLoader methods directly, but it runs within real Hazelcast instance
 */
@QuickTest
@SerialTest
public class H2GenericMapLoaderTest extends GenericMapLoaderTest {

    @BeforeAll
    public static void beforeClass() {
        initialize(new H2DatabaseProvider());
    }

}
