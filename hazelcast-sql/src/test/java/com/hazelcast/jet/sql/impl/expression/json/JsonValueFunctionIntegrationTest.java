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

import java.io.Serializable;
import java.math.BigDecimal;

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
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.byteField') FROM test" ,
                rows(1, "1"));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.shortField') FROM test" ,
                rows(1, "2"));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.intField') FROM test" ,
                rows(1, "3"));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.longField') FROM test" ,
                rows(1, "4"));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.stringField') FROM test" ,
                rows(1, "6"));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.charField') FROM test" ,
                rows(1, "7"));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.floatField') FROM test" ,
                rows(1, "8.0"));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.doubleField') FROM test" ,
                rows(1, "9.0"));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.bigDecimalField') FROM test" ,
                rows(1, "1E+1000"));
    }

    @Test
    public void when_calledWithReturning_then_correctTypeIsReturned() {
        initMultiTypeObject();
        createMapping("test", "bigint", "json");
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.byteField' RETURNING TINYINT) FROM test" ,
                rows(1, (byte) 1));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.shortField' RETURNING SMALLINT) FROM test" ,
                rows(1, (short) 2));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.intField' RETURNING INTEGER) FROM test" ,
                rows(1, 3));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.longField' RETURNING BIGINT) FROM test" ,
                rows(1, 4L));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.stringField' RETURNING VARCHAR) FROM test" ,
                rows(1, "6"));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.charField' RETURNING VARCHAR) FROM test" ,
                rows(1, "7"));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.floatField' RETURNING REAL) FROM test" ,
                rows(1, 8.0f));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.doubleField' RETURNING DOUBLE) FROM test" ,
                rows(1, 9.0));
        assertRowsAnyOrder("SELECT JSON_VALUE(this, '$.bigDecimalField' RETURNING DECIMAL) FROM test" ,
                rows(1, new BigDecimal("1e1000")));
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
                .hasMessageContaining("JSON argument is empty");

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
                .hasMessageContaining("Result of JSON_VALUE can not be array or object");
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
                .hasMessageContaining("Result of JSON_VALUE can not be array or object");
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
        createMapping("test", Long.class, ObjectWithJson.class);

        instance().getSql().execute("INSERT INTO test (__key, jsonValue) VALUES (1, '[1,2,3]')");

        assertThatThrownBy(() -> query("SELECT JSON_VALUE(jsonValue, '') FROM test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Invalid JSONPath expression");
        assertThatThrownBy(() -> query("SELECT JSON_VALUE(jsonValue, '$((@@$#229))') FROM test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Invalid JSONPath expression");
        assertThatThrownBy(() -> query("SELECT JSON_VALUE(jsonValue, jsonPath) FROM test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("JSONPath expression can not be null");
    }

    @Test
    public void test_nullLiteral() {
        assertThatThrownBy(() -> query("SELECT JSON_VALUE(null, null)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("JSONPath expression can not be null");
        assertThatThrownBy(() -> query("SELECT JSON_VALUE('foo', null)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("JSONPath expression can not be null");
        assertNull(querySingleValue("SELECT JSON_VALUE(null, 'foo')"));
        assertNull(querySingleValue("SELECT JSON_VALUE('bad json', '$' default null on error)"));
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

    private static class MultiTypeObject {
        public Byte byteField = 1;
        public Short shortField = 2;
        public Integer intField = 3;
        public Long longField = 4L;
        public String stringField = "6";
        public Character charField = '7';
        public Float floatField = 8.0f;
        public Double doubleField = 9.0;
        public BigDecimal bigDecimalField = new BigDecimal("1e1000");
    }

    public static class ObjectWithJson implements Serializable {
        public String jsonValue;
        public String jsonPath;
    }
}
