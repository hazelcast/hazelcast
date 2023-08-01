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

package com.hazelcast.mapstore.mssql;

import com.hazelcast.mapstore.GenericMapStoreTest;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.jdbc.MSSQLDatabaseProvider;
import com.hazelcast.test.jdbc.MySQLDatabaseProvider;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static com.hazelcast.mapstore.GenericMapLoader.DATA_CONNECTION_REF_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.EXTERNAL_NAME_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeFalse;

@Category(NightlyTest.class)
public class MSSQLGenericMapStoreTest extends GenericMapStoreTest {

    @BeforeClass
    public static void beforeClass() {
        initialize(new MSSQLDatabaseProvider());
    }
}
