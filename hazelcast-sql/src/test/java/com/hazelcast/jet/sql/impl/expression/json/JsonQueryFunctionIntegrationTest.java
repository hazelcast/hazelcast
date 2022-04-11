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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * A test for JSON_QUERY function.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JsonQueryFunctionIntegrationTest extends SqlJsonTestSupport {
    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
    }

    @Test
    public void test_string() {
        final IMap<Long, String> test = instance().getMap("test");
        test.put(1L, "[1,2,3]");
        createMapping("test", Long.class, String.class);
        assertRowsAnyOrder("SELECT JSON_QUERY(this, '$[*]?(@ > 1)' WITH ARRAY WRAPPER) FROM test",
                rows(1, json("[2,3]")));
    }

    @Test
    public void test_hazelcastJsonValue() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, json("[1,2,3]"));
        createMapping("test", "bigint", "json");
        assertRowsAnyOrder("SELECT JSON_QUERY(this, '$[*]?(@ > 1)' WITH ARRAY WRAPPER) FROM test",
                rows(1, json("[2,3]")));
    }

    @Test
    public void test_objectWithJsonField() {
        final IMap<Long, ObjectWithJson> test = instance().getMap("test");
        test.put(1L, new ObjectWithJson(1L, "[1,2,3]"));
        createMapping("test", Long.class, ObjectWithJson.class);

        assertRowsAnyOrder("SELECT JSON_QUERY(jsonValue, '$'), id FROM test",
                rows(2, json("[1,2,3]"), 1L));
    }

    @Test
    public void test_extendedSyntax() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, json(""));
        test.put(2L, json("[1,2,"));
        createMapping("test", "bigint", "json");

        assertNull(querySingleValue("SELECT JSON_QUERY(this, '$' NULL ON EMPTY) AS c1 FROM test WHERE __key = 1"));
        assertEquals(json("[]"),
                querySingleValue("SELECT JSON_QUERY(this, '$' EMPTY ARRAY ON EMPTY) AS c1 FROM test WHERE __key = 1"));
        assertEquals(json("{}"),
                querySingleValue("SELECT JSON_QUERY(this, '$' EMPTY OBJECT ON EMPTY) AS c1 FROM test WHERE __key = 1"));
        assertThatThrownBy(() -> query("SELECT JSON_QUERY(this, '$' ERROR ON EMPTY) AS c1 FROM test WHERE __key = 1"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("JSON_QUERY evaluated to no value");

        assertNull(querySingleValue("SELECT JSON_QUERY(this, '$' NULL ON ERROR) AS c1 FROM test WHERE __key = 2"));
        assertEquals(json("[]"),
                querySingleValue("SELECT JSON_QUERY(this, '$' EMPTY ARRAY ON ERROR) AS c1 FROM test WHERE __key = 2"));
        assertEquals(json("{}"),
                querySingleValue("SELECT JSON_QUERY(this, '$' EMPTY OBJECT ON ERROR) AS c1 FROM test WHERE __key = 2"));
        assertThatThrownBy(() -> query("SELECT JSON_QUERY(this, '$' ERROR ON ERROR) AS c1 FROM test WHERE __key = 2"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("JSON_QUERY failed");
    }

    @Test
    public void test_defaultWrapperBehavior() {
        initComplexObject();

        assertEquals(json("[1,\"2\",3,{\"t\":1}]"),
                querySingleValue("SELECT JSON_QUERY(this, '$[0]') FROM test"));
        assertEquals(json("{\"t\":1}"),
                querySingleValue("SELECT JSON_QUERY(this, '$[1]') FROM test"));
        assertEquals(json("3"),
                querySingleValue("SELECT JSON_QUERY(this, '$[2]') FROM test"));
        assertEquals(json("\"foo\""),
                querySingleValue("SELECT JSON_QUERY(this, '$[3]') FROM test"));
    }

    @Test
    public void test_noArrayWrapper() {
        initComplexObject();

        assertEquals(json("[1,\"2\",3,{\"t\":1}]"),
                querySingleValue("SELECT JSON_QUERY(this, '$[0]' WITHOUT WRAPPER) FROM test"));
        assertEquals(json("{\"t\":1}"),
                querySingleValue("SELECT JSON_QUERY(this, '$[1]' WITHOUT WRAPPER) FROM test"));
        assertEquals(json("3"),
                querySingleValue("SELECT JSON_QUERY(this, '$[2]' WITHOUT WRAPPER) FROM test"));
        assertEquals(json("\"foo\""),
                querySingleValue("SELECT JSON_QUERY(this, '$[3]' WITHOUT WRAPPER) FROM test"));
        assertThatThrownBy(() -> querySingleValue("SELECT JSON_QUERY(this, '$[*]' WITHOUT WRAPPER ERROR ON ERROR) "
                + "FROM test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("JSON_QUERY evaluated to multiple values");
    }

    @Test
    public void test_conditionalArrayWrapper() {
        initComplexObject();

        assertEquals(json("[1,\"2\",3,{\"t\":1}]"),
                querySingleValue("SELECT JSON_QUERY(this, '$[0]' WITH CONDITIONAL WRAPPER) FROM test"));
        assertEquals(json("{\"t\":1}"),
                querySingleValue("SELECT JSON_QUERY(this, '$[1]' WITH CONDITIONAL WRAPPER) FROM test"));
        assertEquals(json("3"),
                querySingleValue("SELECT JSON_QUERY(this, '$[2]' WITH CONDITIONAL WRAPPER) FROM test"));
        assertEquals(json("\"foo\""),
                querySingleValue("SELECT JSON_QUERY(this, '$[3]' WITH CONDITIONAL WRAPPER) FROM test"));
    }

    @Test
    public void test_unconditionalArrayWrapper() {
        initComplexObject();

        assertEquals("[[1,\"2\",3,{\"t\":1}]]",
                querySingleValue("SELECT JSON_QUERY(this, '$[0]' WITH UNCONDITIONAL ARRAY WRAPPER) FROM test")
                        .toString());
        assertEquals("[{\"t\":1}]",
                querySingleValue("SELECT JSON_QUERY(this, '$[1]' WITH UNCONDITIONAL ARRAY WRAPPER) FROM test")
                        .toString());
        assertEquals("[3]",
                querySingleValue("SELECT JSON_QUERY(this, '$[2]' WITH UNCONDITIONAL ARRAY WRAPPER ERROR ON ERROR) FROM test")
                        .toString());
    }

    @Test
    public void test_invalidJsonPath() {
        initComplexObject();
        createMapping("test2", Long.class, ObjectWithJson.class);
        instance().getSql().execute("INSERT INTO test2 (__key, id) VALUES (1, 1)");

        assertThatThrownBy(() -> query("SELECT JSON_QUERY(this, '') FROM test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageEndingWith("Invalid SQL/JSON path expression: Unexpected token at line 1, column 0");
        assertThatThrownBy(() -> query("SELECT JSON_QUERY(this, '$((@@$#229))') FROM test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageEndingWith("Invalid SQL/JSON path expression: Unexpected token at line 1, columns 1 to 2");
        assertThatThrownBy(() -> query("SELECT JSON_QUERY('[1,2,3]', jsonValue) FROM test2"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageEndingWith("SQL/JSON path expression cannot be null");
    }

    @Test
    public void when_strictJsonPathMode_then_failNotSupported() {
        initComplexObject();
        createMapping("test2", Long.class, ObjectWithJson.class);
        instance().getSql().execute("INSERT INTO test2 (__key, id) VALUES (1, 1)");

        assertThatThrownBy(() -> query("SELECT JSON_QUERY(this, 'strict $[*]') FROM test"))
            .isInstanceOf(HazelcastSqlException.class)
            .hasMessageEndingWith("Strict SQL/JSON path mode not supported");
    }

    @Test
    public void test_testLaxJsonPathMode() {
        final IMap<Long, String> test = instance().getMap("test");
        test.put(1L, "[1,2,3]");
        createMapping("test", Long.class, String.class);
        assertRowsAnyOrder("SELECT JSON_QUERY(this, 'lax $[*]?(@ > 1)' WITH ARRAY WRAPPER) FROM test",
            rows(1, json("[2,3]")));
    }

    @Test
    public void test_nullLiteral() {
        assertThatThrownBy(() -> query("SELECT JSON_QUERY(null, null)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasRootCauseMessage("SQL/JSON path expression cannot be null");
        assertThatThrownBy(() -> query("SELECT JSON_QUERY('foo', null)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasRootCauseMessage("SQL/JSON path expression cannot be null");
        assertNull(querySingleValue("SELECT JSON_QUERY(null, '$.a')"));
        // this query returns null JSON value, not a null SQL value
        assertEquals(json("null"), querySingleValue("SELECT JSON_QUERY('{\"a\":null}', '$.a')"));
    }

    @Test
    public void test_arrayIndex() {
        final IMap<Long, String> test = instance().getMap("test");
        test.put(1L, "[1,2,3]");
        createMapping("test", Long.class, String.class);
        assertRowsAnyOrder("SELECT JSON_QUERY(this, '$[0 to 2]' WITH ARRAY WRAPPER) FROM test",
            rows(1, json("[1,2,3]")));
        assertRowsAnyOrder("SELECT JSON_QUERY(this, '$[0 to 1]' WITH ARRAY WRAPPER) FROM test",
            rows(1, json("[1,2]")));
    }

    @Test
    public void test_quotedPropName() {
        final IMap<Long, String> test = instance().getMap("test");
        test.put(1L, "{\"first name\":\"value\"}");
        createMapping("test", Long.class, String.class);
        // sql style
        assertRowsAnyOrder("SELECT JSON_QUERY(this, '$.\"first name\"' WITH ARRAY WRAPPER) FROM test",
            rows(1, json("[\"value\"]")));
        // node.js style - not supported
        assertThatThrownBy(() -> query("SELECT JSON_QUERY(this, '$.[\"first name\"]' WITH ARRAY WRAPPER) FROM test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Invalid SQL/JSON path expression: Line 1, column 2: no viable alternative at input '.['");
    }

    @Test
    public void when_singleQuotedString_then_notSupported() {
        final IMap<Long, String> test = instance().getMap("test");
        test.put(1L, "{\"a\":\"b\"}");
        createMapping("test", Long.class, String.class);

        assertThatThrownBy(() -> query("SELECT JSON_QUERY(this, '$.''a''') from test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Line 1, column 2: token recognition error at: '''");
    }

    @Test
    public void test_onEmptyBehavior() {
        initComplexObject();

        // test NULL ON EMPTY as the default option
        assertRowsAnyOrder("SELECT JSON_QUERY(this, '$[4]') from test", rows(1, (Object) null));
        assertRowsAnyOrder("SELECT JSON_QUERY('', '$[4]') from test", rows(1, (Object) null));
        assertRowsAnyOrder("SELECT JSON_QUERY(null, '$[4]') from test", rows(1, (Object) null));

        assertRowsAnyOrder("SELECT JSON_QUERY(this, '$[4]' NULL ON EMPTY) from test", rows(1, (Object) null));
        assertRowsAnyOrder("SELECT JSON_QUERY('', '$[4]' NULL ON EMPTY) from test", rows(1, (Object) null));
        assertRowsAnyOrder("SELECT JSON_QUERY(null, '$[4]' NULL ON EMPTY) from test", rows(1, (Object) null));

        assertThatThrownBy(() -> query("SELECT JSON_QUERY(this, '$[4]' ERROR ON EMPTY) from test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageEndingWith("JSON_QUERY evaluated to no value");
        assertThatThrownBy(() -> query("SELECT JSON_QUERY('', '$[4]' ERROR ON EMPTY) from test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageEndingWith("JSON_QUERY evaluated to no value");
        assertThatThrownBy(() -> query("SELECT JSON_QUERY(null, '$[4]' ERROR ON EMPTY) from test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageEndingWith("JSON_QUERY evaluated to no value");

        assertRowsAnyOrder("SELECT JSON_QUERY(this, '$[4]' EMPTY OBJECT ON EMPTY) from test", rows(1, json("{}")));
        assertRowsAnyOrder("SELECT JSON_QUERY('', '$[4]' EMPTY OBJECT ON EMPTY) from test", rows(1, json("{}")));
        assertRowsAnyOrder("SELECT JSON_QUERY(null, '$[4]' EMPTY OBJECT ON EMPTY) from test", rows(1, json("{}")));

        assertRowsAnyOrder("SELECT JSON_QUERY(this, '$[4]' EMPTY ARRAY ON EMPTY) from test", rows(1, json("[]")));
        assertRowsAnyOrder("SELECT JSON_QUERY('', '$[4]' EMPTY ARRAY ON EMPTY) from test", rows(1, json("[]")));
        assertRowsAnyOrder("SELECT JSON_QUERY(null, '$[4]' EMPTY ARRAY ON EMPTY) from test", rows(1, json("[]")));
    }

    @Test
    public void test_onErrorBehavior() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        createMapping("test", "bigint", "json");
        test.put(1L, json("bad json"));

        assertRowsAnyOrder("SELECT JSON_QUERY(this, '$[4]' NULL ON ERROR) from test", rows(1, (Object) null));

        assertThatThrownBy(() -> query("SELECT JSON_QUERY(this, '$[4]' ERROR ON ERROR) from test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageEndingWith("Failed to parse JSON document");

        assertRowsAnyOrder("SELECT JSON_QUERY(this, '$[4]' EMPTY OBJECT ON ERROR) from test", rows(1, json("{}")));

        assertRowsAnyOrder("SELECT JSON_QUERY(this, '$[4]' EMPTY ARRAY ON ERROR) from test", rows(1, json("[]")));
    }

    @Test
    public void test_jsonPathLikeRegex() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        createMapping("test", "bigint", "json");
        test.put(1L, json("["
            + "\"alpha\","
            + "\"alpha1\","
            + "\"beta\","
            + "\"BETA\","
            + "1,"
            + "22,"
            + "\"foo\","
            + "\"\\\"quoted \\\"\""
            + "]"));

        assertEquals(json("[\"alpha\",\"alpha1\"]"),
            querySingleValue("SELECT JSON_QUERY(this, '$[*]?(@ like_regex \"alpha\")' WITH CONDITIONAL WRAPPER) FROM test"));
        assertEquals(json("\"alpha\""),
            querySingleValue("SELECT JSON_QUERY(this, '$[*]?(@ like_regex \"(alpha)$\")' WITH CONDITIONAL WRAPPER) FROM test"));
        assertEquals(json("[\"beta\",\"BETA\"]"),
            querySingleValue("SELECT JSON_QUERY(this, '$[*]?(@ like_regex \"(?i)beta\")' WITH CONDITIONAL WRAPPER) FROM test"));
        assertEquals(json("[\"alpha1\",1,22]"),
            querySingleValue("SELECT JSON_QUERY(this, '$[*]?(@ like_regex \"\\\\d\")' WITH CONDITIONAL WRAPPER) FROM test"));
        assertEquals(json("22"),
            querySingleValue("SELECT JSON_QUERY(this, '$[*]?(@ like_regex \"\\\\d{2}\")' WITH CONDITIONAL WRAPPER) FROM test"));
        assertEquals(json("[\"alpha\",\"alpha1\",\"beta\",\"BETA\",1,22,\"foo\",\"\\\"quoted \\\"\"]"),
            querySingleValue("SELECT JSON_QUERY(this, '$[*]?(@ like_regex \"\")' WITH CONDITIONAL WRAPPER) FROM test"));
        assertEquals(json("\"\\\"quoted \\\"\""),
            querySingleValue("SELECT JSON_QUERY(this, '$[*]?(@ like_regex \"\\\"quoted\\\\s\\\"\")' WITH CONDITIONAL WRAPPER) FROM test"));
    }

    protected void initComplexObject() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        createMapping("test", "bigint", "json");
        test.put(1L, json("["
                + "[1,\"2\",3,{\"t\":1}],"
                + "{\"t\":1},"
                + "3,"
                + "\"foo\""
                + "]"));
    }

    public static class ObjectWithJson implements Serializable {
        private Long id;
        private String jsonValue;

        public ObjectWithJson() { }

        public ObjectWithJson(final Long id, final String jsonValue) {
            this.id = id;
            this.jsonValue = jsonValue;
        }

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }

        public String getJsonValue() {
            return jsonValue;
        }

        public void setJsonValue(final String jsonValue) {
            this.jsonValue = jsonValue;
        }
    }
}
