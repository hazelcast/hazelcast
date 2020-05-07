/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.schema;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.schema.model.AllTypesValue;
import com.hazelcast.sql.schema.model.Person;
import com.hazelcast.sql.support.CalciteSqlTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SchemaTest extends CalciteSqlTestSupport {

    private static final String TYPE = "PARTITIONED";

    private static HazelcastInstance member;

    @BeforeClass
    public static void beforeClass() {
        member = Hazelcast.newHazelcastInstance();
    }

    @AfterClass
    public static void afterClass() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testSelectFromDeclaredTable() {
        String name = "predeclared_map";
        List<SqlRow> updateRows = getQueryRows(member, format("CREATE EXTERNAL TABLE %s (__key INT) TYPE %s", name, TYPE));
        assertThat(updateRows).hasSize(1);
        assertThat((int) updateRows.get(0).getObject(0)).isEqualTo(0);

        List<SqlRow> queryRows = getQueryRows(member, format("SELECT __key FROM %s", name));
        assertThat(queryRows).isEmpty();
    }

    @Test
    public void testSchemaDistribution() {
        String name = "distributed_schema_map";
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        try {
            HazelcastInstance[] instances = factory.newInstances();

            // create table on one member
            executeQuery(instances[0], format("CREATE EXTERNAL TABLE %s (__key INT) TYPE %s", name, TYPE));

            // execute query on another one
            List<SqlRow> rows = getQueryRows(instances[1], format("SELECT __key FROM %s", name));

            assertThat(rows).isEmpty();
        } finally {
            factory.terminateAll();
        }
    }

    @Test
    public void testPredeclaredTablePriority() {
        String name = "priority_map";
        executeQuery(member, format("CREATE EXTERNAL TABLE %s (\"__key.age\" INT, age INT) TYPE %s", name, TYPE));

        Map<Person, Person> map = member.getMap(name);
        map.put(new Person("Alice", BigInteger.valueOf(30)), new Person("Bob", BigInteger.valueOf(40)));

        List<SqlRow> rows = getQueryRows(member, format("SELECT age, \"__key.age\" FROM %s", name));

        assertThat(rows).hasSize(1);
        assertThat((int) rows.get(0).getObject(0)).isEqualTo(40);
        assertThat((int) rows.get(0).getObject(1)).isEqualTo(30);
    }

    @Test
    public void testSelectAllSupportedTypes() {
        String name = "all_fields_map";
        executeQuery(member, format("CREATE EXTERNAL TABLE %s ("
                        + "__key DECIMAL(10, 0), "
                        + "string VARCHAR,"
                        + "character0 CHAR, "
                        + "boolean0 BOOLEAN, "
                        + "byte0 TINYINT, "
                        + "short0 SMALLINT, "
                        + "int0 INT, "
                        + "long0 BIGINT, "
                        + "float0 REAL, "
                        + "double0 DOUBLE, "
                        + "bigDecimal DEC(10, 1), "
                        + "bigInteger NUMERIC(5, 0), "
                        + "\"localTime\" TIME, "
                        + "localDate DATE, "
                        + "localDateTime TIMESTAMP, "
                        + "\"date\" TIMESTAMP WITH LOCAL TIME ZONE (\"DATE\"), "
                        + "calendar TIMESTAMP WITH TIME ZONE (\"CALENDAR\"), "
                        + "instant TIMESTAMP WITH LOCAL TIME ZONE, "
                        + "zonedDateTime TIMESTAMP WITH TIME ZONE (\"ZONED_DATE_TIME\"), "
                        + "offsetDateTime TIMESTAMP WITH TIME ZONE "
                        /* + "yearMonthInterval INTERVAL_YEAR_MONTH, "
                        + "offsetDateTime INTERVAL_DAY_SECOND, "*/
                        + ") TYPE %s",
                name, TYPE
        ));

        AllTypesValue allTypes = new AllTypesValue(
                "string",
                'a',
                true,
                (byte) 127,
                (short) 32767,
                2147483647,
                9223372036854775807L,
                1234567890.2f,
                123451234567890.2,
                new BigDecimal("9223372036854775.123"),
                new BigInteger("9223372036854775222"),
                LocalTime.of(12, 23, 34),
                LocalDate.of(2020, 4, 15),
                LocalDateTime.of(2020, 4, 15, 12, 23, 34, 100_000_000),
                Date.from(Instant.ofEpochMilli(1586953414200L)),
                GregorianCalendar.from(ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 300_000_000, UTC)),
                Instant.ofEpochMilli(1586953414400L),
                ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 500_000_000, UTC),
                OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 600_000_000, UTC)
        );

        Map<BigInteger, AllTypesValue> map = member.getMap(name);
        map.put(BigInteger.valueOf(13), allTypes);

        List<SqlRow> rows = getQueryRows(member, format("SELECT * FROM %s", name));

        assertThat(rows).hasSize(1);
        assertThat((BigInteger) rows.get(0).getObject(0)).isEqualTo(BigInteger.valueOf(13));
        assertThat((String) rows.get(0).getObject(1)).isEqualTo(allTypes.getString());
        assertThat((char) rows.get(0).getObject(2)).isEqualTo(allTypes.getCharacter0());
        assertThat((boolean) rows.get(0).getObject(3)).isEqualTo(allTypes.isBoolean0());
        assertThat((byte) rows.get(0).getObject(4)).isEqualTo(allTypes.getByte0());
        assertThat((short) rows.get(0).getObject(5)).isEqualTo(allTypes.getShort0());
        assertThat((int) rows.get(0).getObject(6)).isEqualTo(allTypes.getInt0());
        assertThat((long) rows.get(0).getObject(7)).isEqualTo(allTypes.getLong0());
        assertThat((float) rows.get(0).getObject(8)).isEqualTo(allTypes.getFloat0());
        assertThat((double) rows.get(0).getObject(9)).isEqualTo(allTypes.getDouble0());
        assertThat((BigDecimal) rows.get(0).getObject(10)).isEqualTo(allTypes.getBigDecimal());
        assertThat((BigInteger) rows.get(0).getObject(11)).isEqualTo(allTypes.getBigInteger());
        assertThat((LocalTime) rows.get(0).getObject(12)).isEqualTo(allTypes.getLocalTime());
        assertThat((LocalDate) rows.get(0).getObject(13)).isEqualTo(allTypes.getLocalDate());
        assertThat((LocalDateTime) rows.get(0).getObject(14)).isEqualTo(allTypes.getLocalDateTime());
        assertThat((Date) rows.get(0).getObject(15)).isEqualTo(allTypes.getDate());
        // TODO:
        assertThat((OffsetDateTime) rows.get(0).getObject(16)).isEqualTo(allTypes.getCalendar().toZonedDateTime().toOffsetDateTime());
        assertThat((Instant) rows.get(0).getObject(17)).isEqualTo(allTypes.getInstant());
        assertThat((ZonedDateTime) rows.get(0).getObject(18)).isEqualTo(allTypes.getZonedDateTime());
        assertThat((OffsetDateTime) rows.get(0).getObject(19)).isEqualTo(allTypes.getOffsetDateTime());
    }

    @Test
    public void testDropTable() {
        String name = "to_be_dropped_map";
        executeQuery(member, format("CREATE EXTERNAL TABLE %s (name VARCHAR) TYPE %s", name, TYPE));
        executeQuery(member, format("DROP EXTERNAL TABLE %s", name));

        assertThatThrownBy(() -> executeQuery(member, format("SELECT name FROM %s", name)))
                .isInstanceOf(HazelcastSqlException.class);
    }
}
