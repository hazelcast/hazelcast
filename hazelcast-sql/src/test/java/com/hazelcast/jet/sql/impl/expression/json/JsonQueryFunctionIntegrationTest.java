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
        initialize(1, null);
    }

    @Test
    public void test_string() {
        final IMap<Long, String> test = instance().getMap("test");
        test.put(1L, "[1,2,3]");
        createMapping("test", Long.class, String.class);
        assertRowsAnyOrder("SELECT JSON_QUERY(this, '$[?(@ > 1)]') FROM test",
                rows(1, json("[2,3]")));
    }

    @Test
    public void test_hazelcastJsonValue() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, json("[1,2,3]"));
        createMapping("test", "bigint", "json");
        assertRowsAnyOrder("SELECT JSON_QUERY(this, '$[?(@ > 1)]') FROM test",
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
                .hasMessageContaining("Empty JSON object");

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

        assertEquals("[1,\"2\",3,{\"t\":1}]",
                querySingleValue("SELECT JSON_QUERY(this, '$[0]') FROM test").toString());
        assertEquals("{\"t\":1}",
                querySingleValue("SELECT JSON_QUERY(this, '$[1]') FROM test").toString());
        assertThatThrownBy(() -> querySingleValue("SELECT JSON_QUERY(this, '$[2]' ERROR ON ERROR) FROM test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("JSON_QUERY result is not an array or object");
    }

    @Test
    public void test_noArrayWrapper() {
        initComplexObject();

        assertEquals("[1,\"2\",3,{\"t\":1}]",
                querySingleValue("SELECT JSON_QUERY(this, '$[0]' WITHOUT ARRAY WRAPPER) FROM test")
                        .toString());
        assertEquals("{\"t\":1}",
                querySingleValue("SELECT JSON_QUERY(this, '$[1]' WITHOUT ARRAY WRAPPER) FROM test")
                        .toString());
        assertThatThrownBy(() -> querySingleValue("SELECT JSON_QUERY(this, '$[2]' WITHOUT ARRAY WRAPPER ERROR ON ERROR) "
                        + "FROM test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("JSON_QUERY result is not an array or object");
    }

    @Test
    public void test_conditionalArrayWrapper() {
        initComplexObject();

        assertEquals("[1,\"2\",3,{\"t\":1}]",
                querySingleValue("SELECT JSON_QUERY(this, '$[0]' WITH CONDITIONAL ARRAY WRAPPER) FROM test")
                        .toString());
        assertEquals("{\"t\":1}",
                querySingleValue("SELECT JSON_QUERY(this, '$[1]' WITH CONDITIONAL ARRAY WRAPPER) FROM test")
                        .toString());
        assertEquals("[3]",
                querySingleValue("SELECT JSON_QUERY(this, '$[2]' WITH CONDITIONAL ARRAY WRAPPER ERROR ON ERROR) FROM test")
                        .toString());
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
                .hasMessageContaining("Invalid JSONPath expression");
        assertThatThrownBy(() -> query("SELECT JSON_QUERY(this, '$((@@$#229))') FROM test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Invalid JSONPath expression");
        assertThatThrownBy(() -> query("SELECT JSON_QUERY('[1,2,3]', jsonValue) FROM test2"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("JSONPath expression can not be null");
    }

    @Test
    public void test_nullLiteral() {
        assertThatThrownBy(() -> query("SELECT JSON_QUERY(null, null)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("JSONPath expression can not be null");
        assertThatThrownBy(() -> query("SELECT JSON_QUERY('foo', null)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("JSONPath expression can not be null");
        assertNull(querySingleValue("SELECT JSON_QUERY(null, 'foo')"));
    }

    protected void initComplexObject() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        createMapping("test", "bigint", "json");
        test.put(1L, json("["
                + "[1,\"2\",3,{\"t\":1}],"
                + "{\"t\":1},"
                + "3"
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
