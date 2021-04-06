package com.hazelcast.sql.impl.expression;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlErrorCode;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class NullIfFunctionIntegrationTest extends ExpressionTestSupport {
    @Test
    public void equalParameters() {
        put(1);

        checkValue0("select nullif(this, 1) from map", SqlColumnType.INTEGER, null);
    }

    @Test
    public void notEqualParameters() {
        put(1);

        checkValue0("select nullif(this, 2) from map", SqlColumnType.INTEGER, 1);
    }

    @Test
    public void numbersCoercion() {
        put(1);

        checkValue0("select nullif(this, 1) from map", SqlColumnType.INTEGER, null);
        checkValue0("select nullif(this, CAST(1 as SMALLINT)) from map", SqlColumnType.INTEGER, null);
        checkValue0("select nullif(this, CAST(1 as BIGINT)) from map", SqlColumnType.BIGINT, null);
        checkValue0("select nullif(this, CAST(1 as REAL)) from map", SqlColumnType.REAL, null);
        checkValue0("select nullif(this, CAST(1 as DOUBLE PRECISION)) from map", SqlColumnType.DOUBLE, null);
        checkValue0("select nullif(this, CAST(1 as DECIMAL)) from map", SqlColumnType.DECIMAL, null);
    }

    @Test
    public void dateTimeValuesAndLiterals() {
        LocalDate localDate = LocalDate.of(2021, 1, 1);
        put(localDate);
        checkValue0("select nullif(this, '2021-01-02') from map", SqlColumnType.DATE, localDate);

        LocalTime localTime = LocalTime.of(12, 0);
        put(localTime);
        checkValue0("select nullif(this, '13:00') from map", SqlColumnType.TIME, localTime);

        LocalDateTime localDateTime = LocalDateTime.of(localDate, localTime);
        put(localDateTime);
        checkValue0("select nullif(this, '2021-01-02T13:00') from map", SqlColumnType.TIMESTAMP, localDateTime);

        OffsetDateTime offsetDateTime = OffsetDateTime.of(localDateTime, ZoneOffset.ofHours(2));
        put(offsetDateTime);
        checkValue0("select nullif(this, '2021-01-02T13:00+01:00') from map", SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, offsetDateTime);
    }

    @Test
    public void fail_whenCantInferNullIfParameterTypes() {
        put(1);
        checkFailure0("select nullif(?, ?) from map", SqlErrorCode.PARSING, "Cannot apply 'NULLIF' function to [UNKNOWN, UNKNOWN] (consider adding an explicit CAST)");
    }
}
