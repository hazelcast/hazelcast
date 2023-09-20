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

package com.hazelcast.mapstore.mariadb;

import com.hazelcast.mapstore.GenericMapStoreIntegrationTest;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.jdbc.MariaDBDatabaseProvider;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@Category({NightlyTest.class})
public class MariaDBGenericMapStoreIntegrationTest extends GenericMapStoreIntegrationTest {

    public MariaDBGenericMapStoreIntegrationTest() {
        setPrefix("maria_");
    }

    // Shadow the parent's @BeforeClass method by using the same method name
    @BeforeClass
    public static void beforeClass() {
        initializeBeforeClass(new MariaDBDatabaseProvider());
    }
}
