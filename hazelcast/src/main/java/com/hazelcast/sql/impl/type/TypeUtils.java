package com.hazelcast.sql.impl.type;

public class TypeUtils {
    /** Precedence of LATE data type. */
    public static final int PRECEDENCE_LATE = 0;

    /** Precedence of BOOLEAN data type. */
    public static final int PRECEDENCE_BOOLEAN = 1;

    /** Precedence of BYTE data type. */
    public static final int PRECEDENCE_BYTE = 2;

    /** Precedence of SHORT data type. */
    public static final int PRECEDENCE_SHORT = 3;

    /** Precedence of INTEGER data type. */
    public static final int PRECEDENCE_INTEGER = 4;

    /** Precedence of LONG data type. */
    public static final int PRECEDENCE_LONG = 5;

    /** Precedence of BIG_INTEGER data type. */
    public static final int PRECEDENCE_BIG_INTEGER = 6;

    /** Precedence of BIG_DECIMAL data type. */
    public static final int PRECEDENCE_BIG_DECIMAL = 7;

    /** Precedence of FLOAT data type. */
    public static final int PRECEDENCE_FLOAT = 8;

    /** Precedence of DOUBLE data type. */
    public static final int PRECEDENCE_DOUBLE = 9;

    /** Precedence of STRING data type. */
    public static final int PRECEDENCE_STRING = 1;

    private TypeUtils() {
        // No-op.
    }
}
