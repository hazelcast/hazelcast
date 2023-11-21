/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.jdbc;

import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;

import static java.util.Arrays.asList;
import static org.assertj.core.util.Lists.newArrayList;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AllTypesInsertJdbcSqlConnectorTest extends JdbcSqlTestSupport {

    @Parameterized.Parameter
    public String type;

    @Parameterized.Parameter(1)
    public String mappingType;

    @Parameterized.Parameter(2)
    public String sqlValue;

    @Parameterized.Parameter(3)
    public Object javaValue;

    @Parameterized.Parameter(4)
    public Object jdbcValue;

    @Parameterized.Parameters(name = "type:{0}, mappingType:{1}, sqlValue:{2}, javaValue:{3}, jdbcValue:{4}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {"VARCHAR(10)", "VARCHAR", "'dummy'", "dummy", "dummy"},
                {"BOOLEAN", "BOOLEAN", "TRUE", true, true},
                {"TINYINT", "TINYINT", "1", 1, 1},
                {"SMALLINT", "SMALLINT", "2", 2, 2},
                {"INTEGER", "INTEGER", "3", 3, 3},
                {"BIGINT", "BIGINT", "4", 4L, 4L},
                {"DECIMAL (10,5)", "DECIMAL", "1.12345", new BigDecimal("1.12345"), new BigDecimal("1.12345")},
                {"REAL", "REAL", "1.5", 1.5f, 1.5f},
                {"DOUBLE", "DOUBLE", "1.8", 1.8, 1.8},
                {"DATE", "DATE", "'2022-12-30'", LocalDate.of(2022, 12, 30), Date.valueOf("2022-12-30")},
                {"TIME", "TIME", "'23:59:59'", LocalTime.of(23, 59, 59), Time.valueOf("23:59:59")},
                {"TIMESTAMP", "TIMESTAMP", "'2022-12-30 23:59:59'",
                        LocalDateTime.of(2022, 12, 30, 23, 59, 59), Timestamp.valueOf("2022-12-30 23:59:59")},
                {"TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE", "'2022-12-30T23:59:59-05:00'",
                        OffsetDateTime.of(2022, 12, 30, 23, 59, 59, 0, ZoneOffset.ofHours(-5)),
                        OffsetDateTime.of(2022, 12, 30, 23, 59, 59, 0, ZoneOffset.ofHours(-5))},
        });
    }

    @BeforeClass
    public static void beforeClass() {
        initialize(new H2DatabaseProvider());
    }

    @Test
    public void insertRowWithAllTypes() throws Exception {
        String tableName = randomTableName();
        createTable(tableName, "id INT", "table_column " + type);

        String mappingName = "mapping_" + randomName();
        execute("CREATE MAPPING " + mappingName
                + " EXTERNAL NAME " + tableName
                + " ("
                + "id INT, "
                + "table_column " + mappingType
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("INSERT INTO " + mappingName + " VALUES(0, " + sqlValue + ")");
        execute("INSERT INTO " + mappingName + " VALUES(1, ?)", javaValue);

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, jdbcValue.getClass()),
                new Row(0, jdbcValue),
                new Row(1, jdbcValue)
        );
    }
}
