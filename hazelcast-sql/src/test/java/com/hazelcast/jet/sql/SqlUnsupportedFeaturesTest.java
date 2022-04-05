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

import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.QueryException;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlUnsupportedFeaturesTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_fullJoin() {
        TestBatchSqlConnector.create(sqlService, "b", 0);

        assertThatThrownBy(() -> sqlService.execute("SELECT 1 FROM b AS b1 FULL JOIN b AS b2 ON b1.v = b2.v"))
                .hasCauseInstanceOf(QueryException.class)
                .hasMessageContaining("FULL join not supported");

        assertThatThrownBy(() -> sqlService.execute("SELECT 1 FROM b AS b1 FULL OUTER JOIN b AS b2 ON b1.v = b2.v"))
                .hasCauseInstanceOf(QueryException.class)
                .hasMessageContaining("FULL join not supported");
    }

    @Test
    public void test_multiFullJoin() {
        TestBatchSqlConnector.create(sqlService, "b", 0);

        assertThatThrownBy(() -> sqlService.execute(
                "SELECT 1 FROM b AS b1 JOIN b AS b2 ON b1.v = b2.v FULL OUTER JOIN b AS b3 ON b2.v = b3.v"))
                .hasCauseInstanceOf(QueryException.class)
                .hasMessageContaining("FULL join not supported");

        assertThatThrownBy(() -> sqlService.execute(
                "SELECT 1 FROM b AS b1 JOIN b AS b2 ON b1.v = b2.v FULL JOIN b AS b3 ON b2.v = b3.v"))
                .hasCauseInstanceOf(QueryException.class)
                .hasMessageContaining("FULL join not supported");
    }

    @Test
    public void test_mapValueConstructor() {
        TestBatchSqlConnector.create(sqlService, "b", 1);

        assertThatThrownBy(() -> sqlService.execute("SELECT MAP[1, 2] FROM b"))
                .hasMessageContaining("MAP VALUE CONSTRUCTOR not supported");
    }

    @Test
    public void test_insert() {
        TestBatchSqlConnector.create(sqlService, "b", 1);

        assertThatThrownBy(() -> sqlService.execute("INSERT INTO b VALUES(1)"))
                .hasMessageContaining("INSERT INTO not supported for TestBatch");
    }

    @Test
    public void test_sink() {
        TestBatchSqlConnector.create(sqlService, "b", 1);

        assertThatThrownBy(() -> sqlService.execute("SINK INTO b VALUES(1)"))
                .hasMessageContaining("SINK INTO not supported for TestBatch");
    }

    @Test
    public void test_update_noPrimaryKey() {
        TestBatchSqlConnector.create(sqlService, "b", 1);

        assertThatThrownBy(() -> sqlService.execute("UPDATE b SET v = 1"))
                .hasMessageContaining("PRIMARY KEY not supported by connector: TestBatch");
    }

    @Test
    public void test_update_fromSelect() {
        createMapping("m1", int.class, int.class);
        instance().getMap("m1").put(1, 1);

        createMapping("m2", int.class, int.class);
        instance().getMap("m2").put(1, 2);

        assertThatThrownBy(() -> sqlService.execute("UPDATE m1 SET __key = (select m2.this from m2 WHERE m1.__key = m2.__key)"))
                .hasMessageContaining("UPDATE FROM SELECT not supported");
    }

    @Test
    public void test_delete_noPrimaryKey() {
        TestBatchSqlConnector.create(sqlService, "b", 1);

        assertThatThrownBy(() -> sqlService.execute("DELETE FROM b WHERE v=1"))
                .hasMessageContaining("PRIMARY KEY not supported by connector: TestBatch");
    }

    @Test
    public void test_upsert() {
        assertThatThrownBy(() -> sqlService.execute("UPSERT INTO foo VALUES(1, 2, 3)"))
                .hasMessageEndingWith("UPSERT is not supported");
    }
}
