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
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JsonQueryIntegrationTest extends SqlTestSupport {
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
        for (final SqlRow row : instance().getSql().execute("SELECT JSON_QUERY(this, '$[?(@ > 1)]') FROM test")) {
            System.out.println(row);
            assertEquals(SqlColumnType.JSON, row.getMetadata().getColumn(0).getType());
            assertEquals(new HazelcastJsonValue("[2,3]"), row.getObject(0));
        }
    }

    @Test
    public void when_jsonIsPassed_queryWorks() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, new HazelcastJsonValue("[1,2,3]"));
        for (final SqlRow row : instance().getSql().execute("SELECT JSON_QUERY(this, '$[?(@ > 1)]') FROM test")) {
            System.out.println(row);
            assertEquals(SqlColumnType.JSON, row.getMetadata().getColumn(0).getType());
            assertEquals(new HazelcastJsonValue("[2,3]"), row.getObject(0));
        }
    }

    @Test
    public void when_mappedJsonIsPassed_queryWorks() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, new HazelcastJsonValue("[1,2,3]"));
        instance().getSql()
                .execute("CREATE MAPPING test (__key BIGINT, this JSON) TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json_type')")
                .updateCount();

        for (final SqlRow row : instance().getSql().execute("SELECT JSON_QUERY(this, '$') FROM test")) {
            System.out.println(row);
            assertEquals(SqlColumnType.JSON, row.getMetadata().getColumn(0).getType());
            assertEquals(new HazelcastJsonValue("[1,2,3]"), row.getObject(0));
        }

        for (final SqlRow row : instance().getSql().execute("SELECT this FROM test")) {
            System.out.println(row);
            assertEquals(SqlColumnType.JSON, row.getMetadata().getColumn(0).getType());
            assertEquals(new HazelcastJsonValue("[1,2,3]"), row.getObject(0));
        }
    }

    @Test
    public void when_complexObjectIsPassed_queryWorks() {
        final IMap<Long, ComplexObject> test = instance().getMap("test");
        test.put(1L, new ComplexObject(1L, new HazelcastJsonValue("[1,2,3]")));
        for (final SqlRow row : instance().getSql().execute("SELECT JSON_QUERY(jsonValue, '$'), id FROM test")) {
            System.out.println(row);
            assertEquals(SqlColumnType.JSON, row.getMetadata().getColumn(0).getType());
            assertEquals(new HazelcastJsonValue("[1,2,3]"), row.getObject(0));
            assertEquals(SqlColumnType.BIGINT, row.getMetadata().getColumn(1).getType());
            assertEquals((Long) 1L, row.getObject(1));
        }
    }

    @Test
    public void when_mappedComplexObjectIsPassed_queryWorks() {
        final IMap<Long, ComplexObject> test = instance().getMap("test");
        test.put(1L, new ComplexObject(1L, new HazelcastJsonValue("[1,2,3]")));

        instance().getSql()
                .execute("CREATE MAPPING test (__key BIGINT, id BIGINT, JsonValue JSON) " +
                        "TYPE IMap " +
                        "OPTIONS ('keyFormat'='bigint', " +
                        "'valueFormat'='java', " +
                        "'valueJavaClass'='com.hazelcast.jet.sql.impl.expression.json.ComplexObject')")
                .updateCount();

        for (final SqlRow row : instance().getSql().execute("SELECT JSON_QUERY(JsonValue, '$'), id FROM test")) {
            System.out.println(row);
            assertEquals(SqlColumnType.JSON, row.getMetadata().getColumn(0).getType());
            assertEquals(new HazelcastJsonValue("[1,2,3]"), row.getObject(0));
            assertEquals(SqlColumnType.BIGINT, row.getMetadata().getColumn(1).getType());
            assertEquals((Long) 1L, row.getObject(1));
        }
    }
}
