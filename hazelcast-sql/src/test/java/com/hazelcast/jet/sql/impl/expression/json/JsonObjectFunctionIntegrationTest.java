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
import com.hazelcast.sql.HazelcastSqlException;
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
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JsonObjectFunctionIntegrationTest extends SqlJsonTestSupport {

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Test
    public void test_basicCreation() {
        assertRowsAnyOrder("SELECT JSON_OBJECT('id': 1, 'name': 'hello')",
                jsonObjRow(objectMap("id", 1, "name", "hello")));
        assertRowsAnyOrder("SELECT JSON_OBJECT(KEY 'id' VALUE 1, 'name' VALUE 'hello')",
                jsonObjRow(objectMap("id", 1, "name", "hello")));
        assertRowsAnyOrder("SELECT JSON_OBJECT(KEY 'id' VALUE 1, 'name' VALUE null NULL ON NULL)",
                jsonObjRow(objectMap("id", 1, "name", null)));
        assertRowsAnyOrder("SELECT JSON_OBJECT(KEY 'id' VALUE 1, 'name' VALUE null ABSENT ON NULL)",
                jsonObjRow(objectMap("id", 1)));
    }

    @Test
    public void test_creationFromTableColumns() {
        createMapping("test", Long.class, ClassObj.class);
        instance().getSql().execute("INSERT INTO test VALUES (1, 1, 'testValue')");
        instance().getSql().execute("INSERT INTO test (__key, id) VALUES (2, 2)");

        assertRowsAnyOrder("SELECT JSON_OBJECT('objId': id, 'objName': name) FROM test WHERE id = 1",
                jsonObjRow(objectMap("objId", 1L, "objName", "testValue")));
        assertRowsAnyOrder("SELECT JSON_OBJECT(KEY 'objId' VALUE id, 'objName' VALUE name) FROM test WHERE id = 1",
                jsonObjRow(objectMap("objId", 1L, "objName", "testValue")));
        assertRowsAnyOrder("SELECT JSON_OBJECT('objId': id, 'objName': name NULL ON NULL) "
                        + "FROM test WHERE id = 2",
                jsonObjRow(objectMap("objId", 2L, "objName", null)));
        assertRowsAnyOrder("SELECT JSON_OBJECT(KEY 'objId' VALUE id, 'objName' VALUE name ABSENT ON NULL)"
                        + " FROM test WHERE id = 2",
                jsonObjRow(objectMap("objId", 2L)));
    }

    @Test
    public void test_nestedJson() {
        assertRowsAnyOrder("select json_object("
                        + "'a' : cast('\"foo\"' as json)," // json string inside an SQL string
                        + "'b' : cast('42' as json)," // json number
                        + "'c' : cast('[1,2,3]' as json))", // json array
                rows(1, json("{\"a\":\"foo\",\"b\":42,\"c\":[1,2,3]}")));
    }

    @Test
    public void when_badInputType_then_fail() {
        assertThatThrownBy(() -> query("select json_object(1:2)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageEndingWith("The type of keys must be VARCHAR");
    }

    @Test
    public void when_keyWithoutValue_then_fail() {
        assertThatThrownBy(() -> query("select json_object(key \"foo\")"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Encountered \")\" at line 1, column 29.");
    }

    @Test
    public void test_dateTimeFormats() {
        final LocalTime time = LocalTime.of(13, 0, 0);
        final LocalDate date = LocalDate.of(2020, 1, 1);
        final LocalDateTime dateTime = LocalDateTime
                .of(2020, 1, 1, 13, 0, 0);
        final OffsetDateTime dateTimeTz = OffsetDateTime.of(dateTime, ZoneOffset.UTC);

        assertRowsAnyOrder("SELECT JSON_OBJECT('a':?, 'b':?, 'c':?, 'd':?)",
                Arrays.asList(time, date, dateTime, dateTimeTz),
                jsonObjRow(objectMap("a", "13:00", "b", "2020-01-01", "c", "2020-01-01T13:00", "d", "2020-01-01T13:00Z")));
    }

    private List<Row> jsonObjRow(final Map<Object, Object> values) {
        return rows(1, json(jsonString(values)));
    }

    public static final class ClassObj implements Serializable {
        public Long id;
        public String name;

        public ClassObj() {
        }

        public ClassObj(final Long id, final String name) {
            this.id = id;
            this.name = name;
        }
    }
}
