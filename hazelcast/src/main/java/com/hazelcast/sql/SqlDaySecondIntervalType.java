package com.hazelcast.sql;

/**
 * SQL interval data type used in SQL processing.
 */
public enum SqlDaySecondIntervalType {
    DAY(0),
    HOUR(1),
    MINUTE(2),
    SECOND(3),
    DAY_TO_HOUR(4),
    DAY_TO_MINUTE(5),
    DAY_TO_SECOND(6),
    HOUR_TO_MINUTE(7),
    HOUR_TO_SECOND(8),
    MINUTE_TO_SECOND(9);

    private final int order;

    private static SqlDaySecondIntervalType[] ALL = new SqlDaySecondIntervalType[10];

    static {
        for (SqlDaySecondIntervalType val : SqlDaySecondIntervalType.values())
            ALL[val.order] = val;
    }

    SqlDaySecondIntervalType(int order) {
        this.order = order;
    }

    public int order() {
        return order;
    }

    public static SqlDaySecondIntervalType byOrder(int order) {
        return ALL[order];
    }
}
