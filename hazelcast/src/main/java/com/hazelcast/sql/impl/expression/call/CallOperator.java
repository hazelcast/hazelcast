package com.hazelcast.sql.impl.expression.call;

/**
 * Function operator.
 */
public class CallOperator {
    /** Plus function. */
    public static final int PLUS = 0;

    /** Character length. */
    public static final int CHAR_LENGTH = 1;

    private CallOperator() {
        // No-op.
    }
}
