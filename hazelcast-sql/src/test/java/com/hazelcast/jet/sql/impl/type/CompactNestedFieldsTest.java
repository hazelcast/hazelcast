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

package com.hazelcast.jet.sql.impl.type;

import com.hazelcast.config.Config;
import com.hazelcast.jet.sql.SqlJsonTestSupport;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
public class CompactNestedFieldsTest extends SqlJsonTestSupport {

    @BeforeClass
    public static void beforeClass() {
        final Config config = new Config();
        config.getJetConfig().setEnabled(true);
        config.getSerializationConfig().getCompactSerializationConfig().setEnabled(true);
        initialize(1, config);
    }

    @Test
    public void test_basicQuerying() {
        instance().getSql().execute("CREATE TYPE Office ("
                + "id BIGINT, "
                + "name VARCHAR "
                + ") OPTIONS ('format'='compact', 'compactTypeName'='Office')");

        instance().getSql().execute("CREATE TYPE Organization ("
                + "id BIGINT, "
                + "name VARCHAR, "
                + "office Office"
                + ") OPTIONS ('format'='compact', 'compactTypeName'='Organization')");

        instance().getSql().execute(
                "CREATE MAPPING test ("
                        + "__key BIGINT,"
                        + "id BIGINT, "
                        + "name VARCHAR, "
                        + "organization Organization"
                        + ")"
                        + "TYPE IMap "
                        + "OPTIONS ("
                        + "'keyFormat'='bigint',"
                        + "'valueFormat'='compact',"
                        + "'valueCompactTypeName'='user'"
                        + ")");

        // TODO INSERT
        instance().getMap("test").put(1L, GenericRecordBuilder.compact("user")
                .setInt64("id", 1L)
                .setString("name", "user1")
                .setGenericRecord("organization", GenericRecordBuilder.compact("organization")
                        .setInt64("id", 1L)
                        .setString("name", "organization1")
                        .setGenericRecord("office", GenericRecordBuilder.compact("office")
                                .setInt64("id", 1L)
                                .setString("name", "office1")
                                .build())
                        .build())
                .build());

        assertRowsAnyOrder("SELECT (organization).office.name FROM test", rows(1, "office1"));
    }
}
