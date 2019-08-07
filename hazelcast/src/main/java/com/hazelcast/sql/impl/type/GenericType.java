package com.hazelcast.sql.impl.type;

public enum GenericType {
    BIT(true),
    TINYINT(true),
    SMALLINT(true),
    INT(true),
    BIGINT(true),
    DECIMAL(true),
    REAL(true),
    DOUBLE(true),
    VARCHAR(true),
    DATE(false),
    TIME(false),
    TIMESTAMP(false),
    TIMESTAMP_WITH_TIMEZONE(false);

    private final boolean convertToNumeric;

    GenericType(boolean convertToNumeric) {
        this.convertToNumeric = convertToNumeric;
    }

    public boolean isConvertToNumeric() {
        return convertToNumeric;
    }
}
