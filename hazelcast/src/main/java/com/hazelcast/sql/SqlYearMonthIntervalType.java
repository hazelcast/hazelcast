package com.hazelcast.sql;

/**
 * SQL interval data type used in SQL processing.
 */
public enum SqlYearMonthIntervalType {
    MONTH(0),
    YEAR(1),
    YEAR_TO_MONTH(2);

    private final int order;

    private static SqlYearMonthIntervalType[] ALL = new SqlYearMonthIntervalType[3];

    static {
        for (SqlYearMonthIntervalType val : SqlYearMonthIntervalType.values())
            ALL[val.order] = val;
    }

    SqlYearMonthIntervalType(int order) {
        this.order = order;
    }

    public int order() {
        return order;
    }

    public static SqlYearMonthIntervalType byOrder(int order) {
        return ALL[order];
    }
}
