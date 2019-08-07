package com.hazelcast.sql.impl.type;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;

public enum GenericType {
    BIT(Boolean.class),
    TINYINT(Byte.class),
    SMALLINT(Short.class),
    INT(Integer.class),
    BIGINT(Long.class),
    DECIMAL(BigDecimal.class),
    REAL(Float.class),
    DOUBLE(Double.class),
    VARCHAR(String.class),
    DATE(LocalDate.class),
    TIME(LocalTime.class),
    TIMESTAMP(LocalDateTime.class),
    TIMESTAMP_WITH_TIMEZONE(ZonedDateTime.class);

    private final Class clazz;

    GenericType(Class clazz) {
        this.clazz = clazz;
    }

    public Class getClazz() {
        return clazz;
    }
}
