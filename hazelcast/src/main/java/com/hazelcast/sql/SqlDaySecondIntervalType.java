package com.hazelcast.sql;

/**
 * SQL interval data type used in SQL processing.
 */
public enum SqlDaySecondIntervalType {
    DAY(0),
    DAY_TO_HOUR(1),
    DAY_TO_MINUTE(2),
    DAY_TO_SECOND(3),
    HOUR(4),
    HOUR_TO_MINUTE(5),
    HOUR_TO_SECOND(6),
    MINUTE(7),
    MINUTE_TO_SECOND(8),
    SECOND(9);

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
