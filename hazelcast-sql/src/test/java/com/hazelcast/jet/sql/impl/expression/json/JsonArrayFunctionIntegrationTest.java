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

package com.hazelcast.jet.sql.impl.expression.json;

import com.hazelcast.jet.sql.SqlJsonTestSupport;
import com.hazelcast.jet.sql.impl.connector.map.model.AllTypesValue;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JsonArrayFunctionIntegrationTest extends SqlJsonTestSupport {

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Test
    public void test_basicCreation() {
        assertRowsAnyOrder("SELECT JSON_ARRAY(1, 2, 3)",
                jsonArrayRow(1, 2, 3));
        assertRowsAnyOrder("SELECT JSON_ARRAY('a', 2, 'b', 5.0)",
                jsonArrayRow("a", 2, "b", 5.0));
        assertRowsAnyOrder("SELECT JSON_ARRAY(1, null, 3 NULL ON NULL)",
                jsonArrayRow(1, null, 3));
        assertRowsAnyOrder("SELECT JSON_ARRAY('a', null, 'b', null ABSENT ON NULL)",
                jsonArrayRow("a", "b"));
    }

    @Test
    public void test_creationFromTableColumns() {
        createMapping("test", Long.class, ClassObj.class);
        instance().getSql().execute("INSERT INTO test (__key, id, name, a, b, c) "
                + "VALUES (1, 1, 'testValue', 1, 2, 3)");
        instance().getSql().execute("INSERT INTO test (__key, id, a, b) VALUES (2, 2, 1, 2)");

        assertRowsAnyOrder("SELECT JSON_ARRAY(a, b, c) FROM test WHERE __key = 1",
                jsonArrayRow(1, 2, 3));
        assertRowsAnyOrder("SELECT JSON_ARRAY(id, name, a, b, c) FROM test WHERE __key = 1",
                jsonArrayRow(1L, "testValue", 1L, 2L, 3L));

        assertRowsAnyOrder("SELECT JSON_ARRAY(id, name, a, b, c NULL ON NULL) FROM test WHERE __key = 2",
                jsonArrayRow(2L, null, 1L, 2L, null));
        assertRowsAnyOrder("SELECT JSON_ARRAY(id, name, a, b, c ABSENT ON NULL) FROM test WHERE __key = 2",
                jsonArrayRow(2L, 1L, 2L));
    }

    @Test
    public void test_dateTimeFormats_parameter() {
        final LocalTime time = LocalTime.of(13, 0, 0);
        final LocalDate date = LocalDate.of(2020, 1, 1);
        final LocalDateTime dateTime = LocalDateTime
                .of(2020, 1, 1, 13, 0, 0);
        final OffsetDateTime dateTimeTz = OffsetDateTime.of(dateTime, ZoneOffset.UTC);

        assertRowsAnyOrder("SELECT JSON_ARRAY(?, ?, ?, ?)",
                Arrays.asList(time, date, dateTime, dateTimeTz),
                jsonArrayRow("13:00", "2020-01-01", "2020-01-01T13:00", "2020-01-01T13:00Z"));
    }

    @Test
    public void test_dateTimeFormats_column() {
        createMapping("m", Integer.class, AllTypesValue.class);
        instance().getMap("m").put(42, AllTypesValue.testValue());

        assertRowsAnyOrder("SELECT JSON_ARRAY(\"localTime\", \"localDate\", \"localDateTime\", \"date\", " +
                        "\"calendar\", \"instant\", \"zonedDateTime\", \"offsetDateTime\") from m",
                jsonArrayRow(
                        "12:23:34",
                        "2020-04-15",
                        "2020-04-15T12:23:34.001",
                        OffsetDateTime.ofInstant(AllTypesValue.testValue().getDate().toInstant(), ZoneId.systemDefault())
                                .toString(),
                        "2020-04-15T12:23:34.200Z",
                        OffsetDateTime.ofInstant(AllTypesValue.testValue().getInstant(), ZoneId.systemDefault())
                                .toString(),
                        "2020-04-15T12:23:34.200Z",
                        "2020-04-15T12:23:34.200Z"));
    }

    @Test
    public void test_nestedJson() {
        assertRowsAnyOrder("select json_array("
                + "cast('\"foo\"' as json)," // json string inside an SQL string
                + "cast('42' as json)," // json number
                + "cast('[1,2,3]' as json))", // json array
                rows(1, json("[\"foo\",42,[1,2,3]]")));
    }

    private List<Row> jsonArrayRow(final Object ...values) {
        return rows(1, json(jsonString(values)));
    }

    public static final class ClassObj implements Serializable {
        public Long id;
        public String name;
        public Long a;
        public Long b;
        public Long c;

        public ClassObj() {
        }

        public ClassObj(final Long id, final String name, final Long a, final Long b, final Long c) {
            this.id = id;
            this.name = name;
            this.a = a;
            this.b = b;
            this.c = c;
        }
    }
}
