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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.SqlJsonTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * A test for JSON_VALUE function.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JsonValueFunctionIntegrationTest extends SqlJsonTestSupport {
    private static final ObjectMapper SERIALIZER = new ObjectMapper();

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Test
    public void when_calledWithBasicSyntax_then_varcharIsReturned() {
        initMultiTypeObject();
        createMapping("test", "bigint", "json");
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.byteField' error on error) FROM test",
                rows(1, "127"));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.shortField') FROM test",
                rows(1, "32767"));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.intField') FROM test",
                rows(1, "2147483647"));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.longField') FROM test",
                rows(1, "9223372036854775807"));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.stringField') FROM test",
                rows(1, "foo"));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.charField') FROM test",
                rows(1, "c"));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.floatField') FROM test",
                rows(1, "8.1"));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.doubleField') FROM test",
                rows(1, "9.123456789012345E50"));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.bigDecimalField') FROM test",
                rows(1, "Infinity"));
    }

    @Test
    public void when_calledWithReturning_then_correctTypeIsReturned() {
        initMultiTypeObject();
        createMapping("test", "bigint", "json");
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.byteField' RETURNING TINYINT error on error) FROM test",
                rows(1, Byte.MAX_VALUE));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.shortField' RETURNING SMALLINT) FROM test",
                rows(1, Short.MAX_VALUE));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.intField' RETURNING INTEGER) FROM test",
                rows(1, Integer.MAX_VALUE));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.longField' RETURNING BIGINT) FROM test",
                rows(1, Long.MAX_VALUE));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.stringField' RETURNING VARCHAR) FROM test",
                rows(1, "foo"));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.charField' RETURNING VARCHAR) FROM test",
                rows(1, "c"));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.floatField' RETURNING REAL) FROM test",
                rows(1, 8.1f));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.doubleField' RETURNING DOUBLE) FROM test",
                rows(1, 9.123456789012345e50));
        assertThatThrownBy(() -> query("SELECT JSON_VALUE(this, '$.bigDecimalField' RETURNING DECIMAL error on error) FROM test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Cannot convert infinite DOUBLE to DECIMAL");
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.doubleField' RETURNING DECIMAL) FROM test",
                rows(1, new BigDecimal("912345678901234469174827437827584684254557974298624")));
    }

    @Test
    public void test_extendedSyntax() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, json(""));
        test.put(2L, json("[1,2,"));
        createMapping("test", "bigint", "json");

        assertNull(querySingleValue("SELECT JSON_VALUE(this, '$' NULL ON EMPTY) AS c1 FROM test WHERE __key = 1"));
        assertEquals((byte) 1, querySingleValue("SELECT JSON_VALUE(this, '$' DEFAULT 1 ON EMPTY) AS c1 FROM test WHERE __key = 1"));
        assertThatThrownBy(() -> query("SELECT JSON_VALUE(this, '$' ERROR ON EMPTY) AS c1 FROM test WHERE __key = 1"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("JSON_VALUE evaluated to no value");

        assertNull(querySingleValue("SELECT JSON_VALUE(this, '$' NULL ON ERROR) FROM test WHERE __key = 2"));
        assertEquals((byte) 1, querySingleValue("SELECT JSON_VALUE(this, '$' DEFAULT 1 ON ERROR) AS c1 FROM test WHERE __key = 2"));
        assertThatThrownBy(() -> query("SELECT JSON_VALUE(this, '$' ERROR ON ERROR) FROM test WHERE __key = 2"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("JSON_VALUE failed");
    }

    @Test
    public void test_fullExtendedSyntaxExpression() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, json(""));
        test.put(2L, json("[1,2,"));
        test.put(3L, json("[3]"));
        createMapping("test", "bigint", "json");

        assertRowsAnyOrder(
                "SELECT __key, "
                        + "JSON_VALUE(this, '$[0]' "
                        + "RETURNING BIGINT DEFAULT CAST(1 AS BIGINT) ON EMPTY "
                        + "DEFAULT CAST(2 AS BIGINT) ON ERROR) "
                        + "FROM test",
                rows(2, 1L, 1L, 2L, 2L, 3L, 3L)
        );
    }

    @Test
    public void when_arrayIsReturned_errorIsThrown() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, json("[1,2,3]"));
        createMapping("test", "bigint", "json");

        assertNull(querySingleValue("SELECT JSON_VALUE(this, '$' NULL ON ERROR) FROM test"));
        assertEquals((byte) 1, querySingleValue("SELECT JSON_VALUE(this, '$' DEFAULT 1 ON ERROR) FROM test"));
        assertThatThrownBy(() -> query("SELECT JSON_VALUE(this, '$' ERROR ON ERROR) FROM test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Result of JSON_VALUE cannot be array or object");
    }

    @Test
    public void when_objectIsReturned_then_errorIsThrown() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, json("{\"test\":1}"));
        createMapping("test", "bigint", "json");

        assertNull(querySingleValue("SELECT JSON_VALUE(this, '$' NULL ON ERROR) FROM test"));
        assertEquals((byte) 1, querySingleValue("SELECT JSON_VALUE(this, '$' DEFAULT 1 ON ERROR) FROM test"));
        assertThatThrownBy(() -> query("SELECT JSON_VALUE(this, '$' ERROR ON ERROR) FROM test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Result of JSON_VALUE cannot be array or object");
    }

    @Test
    public void when_duplicatedOnEmptyClause_then_errorIsThrown() {
        final String[] sqlQueries = new String[] {
                "SELECT JSON_VALUE('[1]', '$[0]' NULL ON EMPTY DEFAULT 1 ON EMPTY)",
                "SELECT JSON_VALUE('[1]', '$[0]' RETURNING BIGINT ERROR ON EMPTY NULL ON EMPTY)",
                "SELECT JSON_VALUE('[1]', '$[0]' NULL ON ERROR DEFAULT 1 ON EMPTY DEFAULT 2 ON EMPTY)",
                "SELECT JSON_VALUE('[1]', '$[0]' RETURNING BIGINT ERROR ON ERROR ERROR ON EMPTY NULL ON EMPTY)"
        };

        for (final String sql : sqlQueries) {
            assertThatThrownBy(() -> query(sql))
                    .isInstanceOf(HazelcastSqlException.class)
                    .hasMessageContaining("Duplicate ON EMPTY clause in JSON_VALUE call");
        }
    }

    @Test
    public void when_duplicatedOnErrorClause_then_errorIsThrown() {
        final String[] sqlQueries = new String[] {
                "SELECT JSON_VALUE('[1]', '$[0]' NULL ON ERROR DEFAULT 1 ON ERROR)",
                "SELECT JSON_VALUE('[1]', '$[0]' RETURNING BIGINT ERROR ON ERROR NULL ON ERROR)",
                "SELECT JSON_VALUE('[1]', '$[0]' NULL ON EMPTY DEFAULT 1 ON ERROR DEFAULT 2 ON ERROR)",
                "SELECT JSON_VALUE('[1]', '$[0]' RETURNING BIGINT ERROR ON EMPTY ERROR ON ERROR NULL ON ERROR)"
        };

        for (final String sql : sqlQueries) {
            assertThatThrownBy(() -> query(sql))
                    .isInstanceOf(HazelcastSqlException.class)
                    .hasMessageContaining("Duplicate ON ERROR clause in JSON_VALUE call");
        }
    }

    @Test
    public void test_invalidJsonPath() {
        createMapping("test", Long.class, String.class);

        instance().getSql().execute("INSERT INTO test (__key, this) VALUES (1, '[1,2,3]')");

        assertThatThrownBy(() -> query("SELECT JSON_VALUE(this, '') FROM test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageEndingWith("Invalid SQL/JSON path expression: Unexpected token at line 1, column 0");
        assertThatThrownBy(() -> query("SELECT JSON_VALUE(this, '$((@@$#229))') FROM test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageEndingWith("Invalid SQL/JSON path expression: Unexpected token at line 1, columns 1 to 2");
    }

    @Test
    public void test_nullLiteral() {
        assertThatThrownBy(() -> query("SELECT JSON_VALUE(null, null)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasRootCauseMessage("SQL/JSON path expression cannot be null");
        assertThatThrownBy(() -> query("SELECT JSON_VALUE('foo', null)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasRootCauseMessage("SQL/JSON path expression cannot be null");
        assertNull(querySingleValue("SELECT JSON_VALUE(null, '$.foo')"));
        assertNull(querySingleValue("SELECT JSON_VALUE('bad json', '$' default null on error)"));
        // this query extracts a null JSON value, which is returned as a null SQL value
        assertNull(querySingleValue("SELECT JSON_VALUE('{\"a\":null}', '$.a')"));
    }

    @Test
    public void test_quotedPropName() {
        final IMap<Long, String> test = instance().getMap("test");
        test.put(1L, "{\"first name\":\"value\"}");
        createMapping("test", Long.class, String.class);
        assertEquals("value",
            querySingleValue("SELECT JSON_VALUE(this, '$.\"first name\"' DEFAULT 1 ON ERROR) FROM test"));
    }

    @Test
    public void test_nonExistingProp() {
        initMultiTypeObject();
        createMapping("test", "bigint", "json");
        assertNull(querySingleValue(
                "SELECT JSON_VALUE(this, '$.nonExistingProperty' NULL ON EMPTY DEFAULT 2 ON ERROR) "
                        + "AS c1 FROM test WHERE __key = 1"
        ));
        assertEquals((byte) 1, querySingleValue(
                "SELECT JSON_VALUE(this, '$.nonExistingProperty' DEFAULT 1 ON EMPTY DEFAULT 2 ON ERROR) "
                        + "AS c1 FROM test WHERE __key = 1"
        ));
        assertThatThrownBy(() -> query(
                "SELECT JSON_VALUE(this, '$.nonExistingProperty' ERROR ON EMPTY DEFAULT 2 ON ERROR) "
                        + "AS c1 FROM test WHERE __key = 1"
        )).isInstanceOf(HazelcastSqlException.class).hasMessageContaining("JSON_VALUE evaluated to no value");
    }

    @Test
    public void test_returningDateTypes() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        final String jsonStr = "["
                + "\"2020-01-01\","
                + "\"2020-01-01 13:00:00\","
                + "\"13:00:00\","
                + "\"2020-01-01T13:00:00Z\""
                + "]";
        test.put(1L, json(jsonStr));
        createMapping("test", "bigint", "json");

        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$[0]' RETURNING DATE) FROM test",
                rows(1, LocalDate.of(2020, 1, 1)));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$[1]' RETURNING TIMESTAMP) FROM test",
                rows(1, LocalDateTime.of(2020, 1, 1, 13, 0, 0)));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$[2]' RETURNING TIME) FROM test",
                rows(1, LocalTime.of(13, 0, 0)));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$[3]' RETURNING TIMESTAMP WITH TIME ZONE) FROM test",
                rows(1, OffsetDateTime.of(2020, 1, 1, 13, 0, 0, 0, ZoneOffset.UTC)));
    }

    private void initMultiTypeObject() {
        final MultiTypeObject value = new MultiTypeObject();
        final String serializedValue;
        try {
            serializedValue = SERIALIZER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new HazelcastException(e);
        }
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, new HazelcastJsonValue(serializedValue));
    }

    @SuppressWarnings("unused")
    private static class MultiTypeObject {
        public Byte byteField = Byte.MAX_VALUE;
        public Short shortField = Short.MAX_VALUE;
        public Integer intField = Integer.MAX_VALUE;
        public Long longField = Long.MAX_VALUE;
        public String stringField = "foo";
        public Character charField = 'c';
        public Float floatField = 8.1f;
        public Double doubleField = 9.123456789012345e50;
        public BigDecimal bigDecimalField = new BigDecimal("1e1000");
    }
}
