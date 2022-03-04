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

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.SqlService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.LocalDate;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.DATE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.SMALLINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIME;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TINYINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.VARCHAR;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class SqlInsertWindowBoundsTest extends SqlTestSupport {
    private SqlService sqlService;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Before
    public void before() throws Exception {
        sqlService = instance().getSql();
    }

    @Test
    public void test_windowBoundsSameTypeAsDescriptor_TINYINT() {
        String name = randomName();
        TestStreamSqlConnector.create(
                sqlService,
                name,
                asList("ts", "name"),
                asList(TINYINT, VARCHAR),
                row((byte) 1, "Alice"),
                row((byte) 10, null)
        );

        String interval = "1";

        final SqlRow row = instance().getSql().execute(sql(name, interval)).iterator().next();
        final SqlRowMetadata metadata = row.getMetadata();

        assertThat(metadata.getColumn(0).getType()).isEqualTo(SqlColumnType.TINYINT);
        assertThat((Object) row.getObject(0)).isInstanceOf(SqlColumnType.TINYINT.getValueClass());

        assertThat(metadata.getColumn(1).getType()).isEqualTo(SqlColumnType.TINYINT);
        assertThat((Object) row.getObject(1)).isInstanceOf(SqlColumnType.TINYINT.getValueClass());

        assertThat(metadata.getColumn(2).getType()).isEqualTo(SqlColumnType.TINYINT);
        assertThat((Object) row.getObject(2)).isInstanceOf(SqlColumnType.TINYINT.getValueClass());
    }

    @Test
    public void test_windowBoundsSameTypeAsDescriptor_SMALLINT() {
        String name = randomName();
        TestStreamSqlConnector.create(
                sqlService,
                name,
                asList("ts", "name"),
                asList(SMALLINT, VARCHAR),
                row((short) 1, "Alice"),
                row((short) 10, null)
        );

        String interval = "1";

        final SqlRow row = instance().getSql().execute(sql(name, interval)).iterator().next();
        final SqlRowMetadata metadata = row.getMetadata();

        assertThat(metadata.getColumn(0).getType()).isEqualTo(SqlColumnType.SMALLINT);
        assertThat((Object) row.getObject(0)).isInstanceOf(SqlColumnType.SMALLINT.getValueClass());

        assertThat(metadata.getColumn(1).getType()).isEqualTo(SqlColumnType.SMALLINT);
        assertThat((Object) row.getObject(1)).isInstanceOf(SqlColumnType.SMALLINT.getValueClass());

        assertThat(metadata.getColumn(2).getType()).isEqualTo(SqlColumnType.SMALLINT);
        assertThat((Object) row.getObject(2)).isInstanceOf(SqlColumnType.SMALLINT.getValueClass());
    }

    @Test
    public void test_windowBoundsSameTypeAsDescriptor_INT() {
        String name = randomName();
        TestStreamSqlConnector.create(
                sqlService,
                name,
                asList("ts", "name"),
                asList(INTEGER, VARCHAR),
                row(1, "Alice"),
                row(10, null)
        );

        String interval = "1";

        final SqlRow row = instance().getSql().execute(sql(name, interval)).iterator().next();
        final SqlRowMetadata metadata = row.getMetadata();

        assertThat(metadata.getColumn(0).getType()).isEqualTo(SqlColumnType.INTEGER);
        assertThat((Object) row.getObject(0)).isInstanceOf(SqlColumnType.INTEGER.getValueClass());

        assertThat(metadata.getColumn(1).getType()).isEqualTo(SqlColumnType.INTEGER);
        assertThat((Object) row.getObject(1)).isInstanceOf(SqlColumnType.INTEGER.getValueClass());

        assertThat(metadata.getColumn(2).getType()).isEqualTo(SqlColumnType.INTEGER);
        assertThat((Object) row.getObject(2)).isInstanceOf(SqlColumnType.INTEGER.getValueClass());
    }

    @Test
    public void test_windowBoundsSameTypeAsDescriptor_BIGINT() {
        String name = randomName();
        TestStreamSqlConnector.create(
                sqlService,
                name,
                asList("ts", "name"),
                asList(BIGINT, VARCHAR),
                row(1L, "Alice"),
                row(10L, null)
        );

        String interval = "1";

        final SqlRow row = instance().getSql().execute(sql(name, interval)).iterator().next();
        final SqlRowMetadata metadata = row.getMetadata();

        assertThat(metadata.getColumn(0).getType()).isEqualTo(SqlColumnType.BIGINT);
        assertThat((Object) row.getObject(0)).isInstanceOf(SqlColumnType.BIGINT.getValueClass());

        assertThat(metadata.getColumn(1).getType()).isEqualTo(SqlColumnType.BIGINT);
        assertThat((Object) row.getObject(1)).isInstanceOf(SqlColumnType.BIGINT.getValueClass());

        assertThat(metadata.getColumn(2).getType()).isEqualTo(SqlColumnType.BIGINT);
        assertThat((Object) row.getObject(2)).isInstanceOf(SqlColumnType.BIGINT.getValueClass());
    }

    @Test
    public void test_windowBoundsSameTypeAsDescriptor_TIME() {
        String name = randomName();
        TestStreamSqlConnector.create(
                sqlService,
                name,
                asList("ts", "name"),
                asList(TIME, VARCHAR),
                row(timestamp(0L).toLocalTime().toString(), "Alice"),
                row(timestamp(10L).toLocalTime().toString(), null)
        );

        String interval = "INTERVAL '0.001' SECOND";

        final SqlRow row = instance().getSql().execute(sql(name, interval)).iterator().next();
        final SqlRowMetadata metadata = row.getMetadata();

        assertThat(metadata.getColumn(0).getType()).isEqualTo(SqlColumnType.TIME);
        assertThat((Object) row.getObject(0)).isInstanceOf(SqlColumnType.TIME.getValueClass());

        assertThat(metadata.getColumn(1).getType()).isEqualTo(SqlColumnType.TIME);
        assertThat((Object) row.getObject(1)).isInstanceOf(SqlColumnType.TIME.getValueClass());

        assertThat(metadata.getColumn(2).getType()).isEqualTo(SqlColumnType.TIME);
        assertThat((Object) row.getObject(2)).isInstanceOf(SqlColumnType.TIME.getValueClass());
    }

    @Test
    public void test_windowBoundsSameTypeAsDescriptor_DATE() {
        String name = randomName();
        TestStreamSqlConnector.create(
                sqlService,
                name,
                asList("ts", "name"),
                asList(DATE, VARCHAR),
                row(LocalDate.of(1970, 1, 1).toString(), "Alice"),
                row(LocalDate.of(1970, 1, 5).toString(), null)
        );

        String interval = "INTERVAL '1' DAY";

        final SqlRow row = instance().getSql().execute(sql(name, interval)).iterator().next();
        final SqlRowMetadata metadata = row.getMetadata();

        assertThat(metadata.getColumn(0).getType()).isEqualTo(SqlColumnType.DATE);
        assertThat((Object) row.getObject(0)).isInstanceOf(SqlColumnType.DATE.getValueClass());

        assertThat(metadata.getColumn(1).getType()).isEqualTo(SqlColumnType.DATE);
        assertThat((Object) row.getObject(1)).isInstanceOf(SqlColumnType.DATE.getValueClass());

        assertThat(metadata.getColumn(2).getType()).isEqualTo(SqlColumnType.DATE);
        assertThat((Object) row.getObject(2)).isInstanceOf(SqlColumnType.DATE.getValueClass());
    }

    @Test
    public void test_windowBoundsSameTypeAsDescriptor_TIMESTAMP() {
        String name = randomName();
        TestStreamSqlConnector.create(
                sqlService,
                name,
                asList("ts", "name"),
                asList(TIMESTAMP, VARCHAR),
                row(timestamp(0L), "Alice"),
                row(timestamp(15L), null)
        );

        String interval = "INTERVAL '0.001' SECOND";

        final SqlRow row = instance().getSql().execute(sql(name, interval)).iterator().next();
        final SqlRowMetadata metadata = row.getMetadata();

        assertThat(metadata.getColumn(0).getType()).isEqualTo(SqlColumnType.TIMESTAMP);
        assertThat((Object) row.getObject(0)).isInstanceOf(SqlColumnType.TIMESTAMP.getValueClass());

        assertThat(metadata.getColumn(1).getType()).isEqualTo(SqlColumnType.TIMESTAMP);
        assertThat((Object) row.getObject(1)).isInstanceOf(SqlColumnType.TIMESTAMP.getValueClass());

        assertThat(metadata.getColumn(2).getType()).isEqualTo(SqlColumnType.TIMESTAMP);
        assertThat((Object) row.getObject(2)).isInstanceOf(SqlColumnType.TIMESTAMP.getValueClass());
    }

    @Test
    public void test_windowBoundsSameTypeAsDescriptor_TIMESTAMP_TZ() {
        String name = randomName();
        TestStreamSqlConnector.create(
                sqlService,
                name,
                asList("ts", "name"),
                asList(TIMESTAMP_WITH_TIME_ZONE, VARCHAR),
                row(timestampTz(0L).toString(), "Alice"),
                row(timestampTz(10L).toString(), null)
        );

        String interval = "INTERVAL '0.001' SECOND";

        final SqlRow row = instance().getSql().execute(sql(name, interval)).iterator().next();
        final SqlRowMetadata metadata = row.getMetadata();
        Class<?> valueClass = SqlColumnType.TIMESTAMP_WITH_TIME_ZONE.getValueClass();

        assertThat(metadata.getColumn(0).getType()).isEqualTo(SqlColumnType.TIMESTAMP_WITH_TIME_ZONE);
        assertThat((Object) row.getObject(0)).isInstanceOf(valueClass);

        assertThat(metadata.getColumn(1).getType()).isEqualTo(SqlColumnType.TIMESTAMP_WITH_TIME_ZONE);
        assertThat((Object) row.getObject(1)).isInstanceOf(valueClass);

        assertThat(metadata.getColumn(2).getType()).isEqualTo(SqlColumnType.TIMESTAMP_WITH_TIME_ZONE);
        assertThat((Object) row.getObject(2)).isInstanceOf(valueClass);
    }


    private String sql(String name, String interval) {
        return "SELECT ts, window_start, window_end FROM " +
                "TABLE(TUMBLE(" +
                "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), " +
                interval +
                ")))" +
                "  , DESCRIPTOR(ts)" +
                "  , " + interval +
                ")) " +
                "GROUP BY window_start, window_end, ts, name";
    }
}
