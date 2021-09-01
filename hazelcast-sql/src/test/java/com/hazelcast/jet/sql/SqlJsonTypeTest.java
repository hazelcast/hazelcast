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

package com.hazelcast.jet.sql;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static java.util.Arrays.asList;

@Category(SlowTest.class)
public class SqlJsonTypeTest extends SqlJsonTestSupport {
    @BeforeClass
    public static void beforeClass() {
        final Config config = new Config();
        config.getJetConfig().setEnabled(true);
        initializeWithClient(1, config, new ClientConfig());
    }

    @Test
    public void when_insertedIntoExistingMap_typeIsCorrect() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        execute("CREATE MAPPING test TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json')");

        test.put(1L, json("[1,2,3]"));
        assertRowsWithType("SELECT * FROM test" ,
                asList(SqlColumnType.BIGINT, SqlColumnType.JSON),
                rows(2, 1L, json("[1,2,3]")));

        execute("INSERT INTO test VALUES (2, CAST('[4,5,6]' AS JSON))");
        assertRowsWithType("SELECT * FROM test" ,
                asList(SqlColumnType.BIGINT, SqlColumnType.JSON),
                rows(2, 1L, json("[1,2,3]"), 2L, json("[4,5,6]")));

        execute("DELETE FROM test WHERE __key = 1");
        assertRowsWithType("SELECT * FROM test" ,
                asList(SqlColumnType.BIGINT, SqlColumnType.JSON),
                rows(2, 2L, json("[4,5,6]")));
    }

    @Test
    public void when_insertNewIntoMappingBasedMap_typeIsCorrect() {
        execute("CREATE MAPPING test (__key BIGINT, this JSON) "
                + "TYPE IMap "
                + "OPTIONS ('keyFormat'='bigint', 'valueFormat'='json')");

        execute("INSERT INTO test VALUES (1, CAST('[1,2,3]' AS JSON))");
        assertRowsWithType("SELECT * FROM test" ,
                asList(SqlColumnType.BIGINT, SqlColumnType.JSON),
                rows(2, 1L, json("[1,2,3]")));

        execute("INSERT INTO test VALUES (2, CAST('[4,5,6]' AS JSON))");
        assertRowsWithType("SELECT * FROM test" ,
                asList(SqlColumnType.BIGINT, SqlColumnType.JSON),
                rows(2, 1L, json("[1,2,3]"), 2L, json("[4,5,6]")));

        execute("DELETE FROM test WHERE __key = 1");
        assertRowsWithType("SELECT * FROM test" ,
                asList(SqlColumnType.BIGINT, SqlColumnType.JSON),
                rows(2, 2L, json("[4,5,6]")));
    }

    @Test
    public void when_sinkIsUsedWithExistingMap_typeIsCorrect() {
        execute("CREATE MAPPING test (__key BIGINT, this JSON) "
                + "TYPE IMap "
                + "OPTIONS ('keyFormat'='bigint', 'valueFormat'='json')");

        execute("INSERT INTO test VALUES (1, CAST('[1,2,3]' AS JSON))");
        assertRowsWithType("SELECT * FROM test" ,
                asList(SqlColumnType.BIGINT, SqlColumnType.JSON),
                rows(2, 1L, json("[1,2,3]")));

        execute("SINK INTO test SELECT 1, CAST('[4,5,6]' AS JSON)");
        assertRowsWithType("SELECT * FROM test" ,
                asList(SqlColumnType.BIGINT, SqlColumnType.JSON),
                rows(2, 1L, json("[4,5,6]")));
    }

    @Test
    public void when_clientIsUsed_typeIsPassedCorrectly() {
        executeClient("CREATE MAPPING test TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json')");

        final IMap<Long, HazelcastJsonValue> test = client().getMap("test");
        test.put(1L, json("[1,2,3]"));

        executeClient("INSERT INTO test VALUES (2, CAST('[4,5,6]' AS JSON))");

        assertRowsWithType(client(),
                "SELECT * FROM test" ,
                asList(SqlColumnType.BIGINT, SqlColumnType.JSON),
                rows(2, 1L, json("[1,2,3]"), 2L, json("[4,5,6]")));
    }
}
