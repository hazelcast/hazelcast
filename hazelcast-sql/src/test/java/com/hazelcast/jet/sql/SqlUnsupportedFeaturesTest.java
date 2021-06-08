/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql;

import com.hazelcast.jet.sql.impl.connector.kafka.KafkaSqlConnector;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.QueryException;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlUnsupportedFeaturesTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_rightJoin() {
        TestBatchSqlConnector.create(sqlService, "b", 0);

        assertThatThrownBy(() -> sqlService.execute("SELECT 1 FROM b AS b1 RIGHT JOIN b AS b2 ON b1.v = b2.v"))
                .hasCauseInstanceOf(QueryException.class)
                .hasMessageContaining("RIGHT join not supported");

        assertThatThrownBy(() -> sqlService.execute("SELECT 1 FROM b AS b1 RIGHT OUTER JOIN b AS b2 ON b1.v = b2.v"))
                .hasCauseInstanceOf(QueryException.class)
                .hasMessageContaining("RIGHT join not supported");
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
    public void test_semiJoin() {
        TestBatchSqlConnector.create(sqlService, "b", 0);

        assertThatThrownBy(() -> sqlService.execute(
                    "SELECT 1 FROM b WHERE EXISTS (SELECT 1 FROM b AS b2 WHERE b.v = b2.v)"))
                .hasCauseInstanceOf(QueryException.class)
                .hasMessageContaining("Function 'EXISTS' does not exist");
    }

    @Test
    public void test_antiJoin() {
        TestBatchSqlConnector.create(sqlService, "b", 0);

        assertThatThrownBy(() -> sqlService.execute(
                    "SELECT 1 FROM b WHERE NOT EXISTS (SELECT 1 FROM b AS b2 WHERE b.v = b2.v)"))
                .hasCauseInstanceOf(QueryException.class)
                .hasMessageContaining("Function 'EXISTS' does not exist");
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
                .hasMessageContaining("INSERT INTO or SINK INTO not supported for TestBatch");
    }

    @Test
    public void test_sink() {
        TestBatchSqlConnector.create(sqlService, "b", 1);

        assertThatThrownBy(() -> sqlService.execute("SINK INTO b VALUES(1)"))
                .hasMessageContaining("INSERT INTO or SINK INTO not supported for TestBatch");
    }

    @Test
    public void test_delete_noDeleteProcessor() {
        sqlService.execute("CREATE MAPPING b ("
                + "__key INT"
                + ", this INT"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='int'"
                + ")"
        );

        assertThatThrownBy(() -> sqlService.execute("DELETE FROM b WHERE v=1"))
                .hasMessageContaining("DELETE not supported by connector: Kafka");
    }

    @Test
    public void test_delete_noPrimaryKey() {
        TestBatchSqlConnector.create(sqlService, "b", 1);

        assertThatThrownBy(() -> sqlService.execute("DELETE FROM b WHERE v=1"))
                .hasMessageContaining("DELETE not supported by connector: TestBatch");
    }
}
