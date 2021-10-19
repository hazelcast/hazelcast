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

import com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector.timestamp;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlEventWatermarkTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() throws IOException {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_watermarks() {
        String name = createTable(
                row(timestamp(0), "Alice"),
                row(timestamp(1), null),
                row(timestamp(2), "Bob")
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " +
                        "TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.001' SECOND))",
                asList(
                        new Row(timestamp(0), "Alice"),
                        new Row(timestamp(1), null),
                        new Row(timestamp(2), "Bob")
                )
        );
    }

    @Test
    public void test_multipleWatermarks() {
        String name = createTable();

        assertThatThrownBy(() -> sqlService.execute(
                "SELECT * FROM " +
                        "TABLE(IMPOSE_ORDER(" +
                        "  (SELECT * FROM" +
                        "    TABLE(IMPOSE_ORDER(" +
                        "      TABLE " + name +
                        "      , DESCRIPTOR(ts)" +
                        "      , INTERVAL '0.001' SECOND" +
                        "    ))" +
                        "  )" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        "))"
        )).hasMessageContaining("Multiple watermarks are not supported");
    }

    @Test
    public void test_watermarksWithFilteredInput() {
        String name = createTable(
                row(timestamp(0), "Alice"),
                row(timestamp(1), null),
                row(timestamp(2), "Bob")
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " +
                        "TABLE(IMPOSE_ORDER(" +
                        "  (SELECT * FROM " + name + " WHERE name != 'Alice')" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.001' SECOND" +
                        "))",
                singletonList(new Row(timestamp(2), "Bob"))
        );
    }

    @Test
    public void test_watermarksWithProjectedInput() {
        String name = createTable(
                row(timestamp(0), "Alice"),
                row(timestamp(1), null),
                row(timestamp(2), "Bob")
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " +
                        "TABLE(IMPOSE_ORDER(" +
                        "  (SELECT ts FROM " + name + ")" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.001' SECOND" +
                        "))",
                asList(
                        new Row(timestamp(0)),
                        new Row(timestamp(1)),
                        new Row(timestamp(2))
                )
        );
    }

    @Test
    public void test_watermarksWithFilteredAndProjectedInput() {
        String name = createTable(
                row(timestamp(0), "Alice"),
                row(timestamp(1), null),
                row(timestamp(2), "Bob")
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " +
                        "TABLE(IMPOSE_ORDER(" +
                        "  (SELECT ts FROM " + name + " WHERE name != 'Alice')" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.001' SECOND" +
                        "))",
                singletonList(new Row(timestamp(2)))
        );
    }

    @Test
    public void test_watermarksWithProjectionThatCannotBePushedDown() {
        String name = createTable();

        assertThatThrownBy(() -> sqlService.execute(
                "SELECT * FROM " +
                        "TABLE(IMPOSE_ORDER(" +
                        "  (SELECT ts + INTERVAL '0.001' SECOND AS ts, name  FROM " + name + ")" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.001' SECOND" +
                        "))"
        )).hasMessageContaining("Watermark function cannot be applied to input table");
    }

    @Test
    public void test_namedParameters() {
        String name = createTable(
                row(timestamp(0), "Alice"),
                row(timestamp(1), null),
                row(timestamp(2), "Bob")
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " +
                        "TABLE(IMPOSE_ORDER(" +
                        "  max_lag => INTERVAL '0.001' SECOND" +
                        "  , input => (TABLE " + name + ")" +
                        "  , time_column => DESCRIPTOR(ts)" +
                        "))",
                asList(
                        new Row(timestamp(0), "Alice"),
                        new Row(timestamp(1), null),
                        new Row(timestamp(2), "Bob")
                )
        );
    }

    private static Object[] row(Object... values) {
        return values;
    }

    private static String createTable(Object[]... values) {
        String name = randomName();
        TestStreamSqlConnector.create(
                sqlService,
                name,
                asList("ts", "name"),
                asList(TIMESTAMP_WITH_TIME_ZONE, VARCHAR),
                values
        );
        return name;
    }
}
