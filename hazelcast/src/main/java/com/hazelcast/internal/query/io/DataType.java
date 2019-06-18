package com.hazelcast.internal.query.io;

public enum DataType {
    INT,
    LONG,
    STRING;

    public static DataType fromOrdinal(int ordinal) {
        for (DataType type : DataType.values())
            if (type.ordinal() == ordinal)
                return type;

        // TODO: Compatibility
        throw new IllegalArgumentException("Unknown ordinal: " + ordinal);
    }
}
