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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.SqlJsonTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JsonQueryIntegrationTest extends SqlJsonTestSupport {
    @BeforeClass
    public static void beforeClass() {
        final Config config = new Config();
        config.getJetConfig().setEnabled(true);
        initialize(1, config);
    }

    @Test
    public void when_stringIsPassed_queryWorks() {
        final IMap<Long, String> test = instance().getMap("test");
        test.put(1L, "[1,2,3]");
        execute("CREATE MAPPING test TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='varchar')");
        assertRowsWithType("SELECT JSON_QUERY(this, '$[?(@ > 1)]') FROM test",
                singletonList(SqlColumnType.JSON),
                rows(1, new HazelcastJsonValue("[2,3]")));
    }

    @Test
    public void when_jsonIsPassed_queryWorks() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, new HazelcastJsonValue("[1,2,3]"));
        execute("CREATE MAPPING test TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json')");
        assertRowsWithType("SELECT JSON_QUERY(this, '$[?(@ > 1)]') FROM test",
                singletonList(SqlColumnType.JSON),
                rows(1, new HazelcastJsonValue("[2,3]")));
    }

    @Test
    public void when_mappedJsonIsPassed_queryWorks() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, new HazelcastJsonValue("[1,2,3]"));
        execute("CREATE MAPPING test (__key BIGINT, this JSON) " +
                        "TYPE IMap " +
                        "OPTIONS ('keyFormat'='bigint', 'valueFormat'='json')");
        assertRowsWithType("SELECT JSON_QUERY(this, '$') FROM test",
                singletonList(SqlColumnType.JSON),
                rows(1, new HazelcastJsonValue("[1,2,3]")));
    }

    @Test
    public void when_complexObjectIsPassed_queryWorks() {
        final IMap<Long, ComplexObject> test = instance().getMap("test");
        test.put(1L, new ComplexObject(1L, "[1,2,3]"));
        execute("CREATE MAPPING test TYPE IMap OPTIONS ("
                + "'keyFormat'='bigint', "
                + "'valueFormat'='java', "
                + "'valueJavaClass'='" + ComplexObject.class.getName() + "')");

        assertRowsWithType("SELECT JSON_QUERY(jsonValue, '$'), id FROM test",
                asList(SqlColumnType.JSON, SqlColumnType.BIGINT),
                rows(2, new HazelcastJsonValue("[1,2,3]"), 1L));
    }

    @Test
    public void when_extendedSyntaxSpecified_queryWorks() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, new HazelcastJsonValue(""));
        test.put(2L, new HazelcastJsonValue("[1,2,"));
        execute("CREATE MAPPING test TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json')");

        assertNull(querySingleValue("SELECT JSON_QUERY(this, '$' NULL ON EMPTY) AS c1 FROM test WHERE __key = 1"));
        assertEquals(new HazelcastJsonValue("[]"),
                querySingleValue("SELECT JSON_QUERY(this, '$' EMPTY ARRAY ON EMPTY) AS c1 FROM test WHERE __key = 1"));
        assertEquals(new HazelcastJsonValue("{}"),
                querySingleValue("SELECT JSON_QUERY(this, '$' EMPTY OBJECT ON EMPTY) AS c1 FROM test WHERE __key = 1"));
        assertThatThrownBy(() -> query("SELECT JSON_QUERY(this, '$' ERROR ON EMPTY) AS c1 FROM test WHERE __key = 1"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Empty JSON object");

        assertNull(querySingleValue("SELECT JSON_QUERY(this, '$' NULL ON ERROR) AS c1 FROM test WHERE __key = 2"));
        assertEquals(new HazelcastJsonValue("[]"),
                querySingleValue("SELECT JSON_QUERY(this, '$' EMPTY ARRAY ON ERROR) AS c1 FROM test WHERE __key = 2"));
        assertEquals(new HazelcastJsonValue("{}"),
                querySingleValue("SELECT JSON_QUERY(this, '$' EMPTY OBJECT ON ERROR) AS c1 FROM test WHERE __key = 2"));
        assertThatThrownBy(() -> query("SELECT JSON_QUERY(this, '$' ERROR ON ERROR) AS c1 FROM test WHERE __key = 2"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("JSON_QUERY failed");
    }

    @Test
    public void when_defaultWrapperBehaviorIsSpecified_queryWorks() {
        initComplexObject();
        execute("CREATE MAPPING test TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json')");
        assertEquals("[1,\"2\",3,{\"t\":1}]",
                querySingleValue("SELECT JSON_QUERY(this, '$[0]') FROM test").toString());
        assertEquals("{\"t\":1}",
                querySingleValue("SELECT JSON_QUERY(this, '$[1]') FROM test").toString());
        assertThatThrownBy(() -> querySingleValue("SELECT JSON_QUERY(this, '$[2]' ERROR ON ERROR) FROM test"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("JSON_QUERY result is not an array or object");
    }

    @Test
    public void when_noArrayWrapperSpecified_queryWorks() {
        initComplexObject();
        execute("CREATE MAPPING test TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json')");

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
    public void when_conditionalArrayWrapperSpecified_queryWorks() {
        initComplexObject();
        execute("CREATE MAPPING test TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json')");

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
    public void when_unconditionalArrayWrapperSpecified_queryWorks() {
        initComplexObject();
        execute("CREATE MAPPING test TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json')");

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
}
