package com.hazelcast.sql.impl.type;

public enum GenericType {
    BIT(true, false),
    TINYINT(true, false),
    SMALLINT(true, false),
    INT(true, false),
    BIGINT(true, false),
    DECIMAL(true, false),
    REAL(true, false),
    DOUBLE(true, false),
    VARCHAR(true, false),
    DATE(false, true),
    TIME(false, true),
    TIMESTAMP(false, true),
    TIMESTAMP_WITH_TIMEZONE(false, true),
    INTERVAL_YEAR_MONTH(false, false),
    INTERVAL_DAY_SECOND(false, false),
    OBJECT(false, false);

    private final boolean convertToNumeric;
    private final boolean temporal;

    GenericType(boolean convertToNumeric, boolean temporal) {
        this.convertToNumeric = convertToNumeric;
        this.temporal = temporal;
    }

    public boolean isConvertToNumeric() {
        return convertToNumeric;
    }

    public boolean isTemporal() {
        return temporal;
    }
}
