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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

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
        executeUpdate(member, format("CREATE EXTERNAL TABLE %s (__key INT) TYPE %s", name, TYPE));

        List<SqlRow> rows = getQueryRows(member, format("SELECT __key FROM %s", name));

        assertEquals(0, rows.size());
    }

    @Test
    public void testSchemaDistribution() {
        String name = "distributed_schema_map";
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        try {
            HazelcastInstance[] instances = factory.newInstances();

            // create table on one member
            executeUpdate(instances[0], format("CREATE EXTERNAL TABLE %s (__key INT) TYPE %s", name, TYPE));

            // execute query on another one
            List<SqlRow> rows = getQueryRows(instances[1], format("SELECT __key FROM %s", name));

            assertEquals(0, rows.size());
        } finally {
            factory.terminateAll();
        }
    }

    @Test
    public void testPredeclaredTablePriority() {
        String name = "priority_map";
        executeUpdate(member, format("CREATE EXTERNAL TABLE %s (\"__key.age\" INT, age INT) TYPE %s", name, TYPE));

        Map<Person, Person> map = member.getMap(name);
        map.put(new Person("Alice", BigInteger.valueOf(30)), new Person("Bob", BigInteger.valueOf(40)));

        List<SqlRow> rows = getQueryRows(member, format("SELECT age, \"__key.age\" FROM %s", name));

        assertEquals(1, rows.size());
        assertEquals(40, ((Integer) rows.get(0).getObject(0)).intValue());
        assertEquals(30, ((Integer) rows.get(0).getObject(1)).intValue());
    }

    @Test
    public void testSelectAllSupportedTypes() {
        String name = "all_fields_map";
        executeUpdate(member, format("CREATE EXTERNAL TABLE %s (" +
                        "__key DECIMAL(10, 0), " +
                        "string VARCHAR," +
                        "character0 CHAR, " +
                        "boolean0 BOOLEAN, " +
                        "byte0 TINYINT, " +
                        "short0 SMALLINT, " +
                        "int0 INT, " +
                        "long0 BIGINT, " +
                        "float0 REAL, " +
                        "double0 DOUBLE, " +
                        "bigDecimal DEC(10, 1), " +
                        "bigInteger NUMERIC(5, 0), " +
                        "\"localTime\" TIME, " +
                        "localDate DATE, " +
                        "localDateTime TIMESTAMP, " +
                        "\"date\" TIMESTAMP WITH LOCAL TIME ZONE (\"DATE\"), " +
                        "\"calendar\" TIMESTAMP WITH TIME ZONE (\"CALENDAR\"), " +
                        "\"instant\" TIMESTAMP WITH LOCAL TIME ZONE, " +
                        "zonedDateTime TIMESTAMP WITH TIME ZONE (\"ZONED_DATE_TIME\"), " +
                        "offsetDateTime TIMESTAMP WITH TIME ZONE " +
                        /*"yearMonthInterval INTERVAL_YEAR_MONTH, " +
                        "offsetDateTime INTERVAL_DAY_SECOND, " +*/
                        ") TYPE %s",
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

        assertEquals(1, rows.size());
        assertEquals(BigInteger.valueOf(13), rows.get(0).getObject(0));
        assertEquals(allTypes.getString(), rows.get(0).getObject(1));
        assertEquals(allTypes.getCharacter0(), ((char) rows.get(0).getObject(2)));
        assertEquals(allTypes.isBoolean0(), rows.get(0).getObject(3));
        assertEquals(allTypes.getByte0(), ((byte) rows.get(0).getObject(4)));
        assertEquals(allTypes.getShort0(), ((short) rows.get(0).getObject(5)));
        assertEquals(allTypes.getInt0(), ((int) rows.get(0).getObject(6)));
        assertEquals(allTypes.getLong0(), ((long) rows.get(0).getObject(7)));
        assertEquals(allTypes.getFloat0(), rows.get(0).getObject(8), 0);
        assertEquals(allTypes.getDouble0(), rows.get(0).getObject(9), 0);
        assertEquals(allTypes.getBigDecimal(), rows.get(0).getObject(10));
        assertEquals(allTypes.getBigInteger(), rows.get(0).getObject(11));
        assertEquals(allTypes.getLocalTime(), rows.get(0).getObject(12));
        assertEquals(allTypes.getLocalDate(), rows.get(0).getObject(13));
        assertEquals(allTypes.getLocalDateTime(), rows.get(0).getObject(14));
        assertEquals(allTypes.getDate(), rows.get(0).getObject(15));
        assertEquals(allTypes.getCalendar().toZonedDateTime().toOffsetDateTime(), rows.get(0).getObject(16)); // TODO:
        assertEquals(allTypes.getInstant(), rows.get(0).getObject(17));
        assertEquals(allTypes.getZonedDateTime(), rows.get(0).getObject(18));
        assertEquals(allTypes.getOffsetDateTime(), rows.get(0).getObject(19));
    }

    @Test
    public void testDropTable() {
        String name = "to_be_dropped_map";
        executeUpdate(member, format("CREATE EXTERNAL TABLE %s (name VARCHAR) TYPE %s", name, TYPE));
        executeUpdate(member, format("DROP EXTERNAL TABLE %s", name));

        assertThatThrownBy(() -> executeQuery(member, format("SELECT name FROM %s", name)))
                .isInstanceOf(HazelcastSqlException.class);
    }
}
