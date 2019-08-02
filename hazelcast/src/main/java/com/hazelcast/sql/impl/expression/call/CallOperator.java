package com.hazelcast.sql.impl.expression.call;

/**
 * Function operator.
 */
public class CallOperator {
    /** Plus function: A + B */
    public static final int PLUS = 0;

    /** Minus function: A - B */
    public static final int MINUS = 1;

    /** Minus function: A * B */
    public static final int MULTIPLY = 3;

    /** Divide function: A / B */
    public static final int DIVIDE = 4;

    /** Divide function: -A */
    public static final int UNARY_MINUS = 5;

    /** Character length. */
    public static final int CHAR_LENGTH = 6;

    private CallOperator() {
        // No-op.
    }
}
