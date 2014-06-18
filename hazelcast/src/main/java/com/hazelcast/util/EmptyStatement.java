package com.hazelcast.util;

/**
 * This class does nothing!
 * <p/>
 * It is useful if you e.g. don't need to do anything with an exception; but checkstyle is complaining that
 * you need to have at least 1 statement.
 */
public final class EmptyStatement {

    //we don't want instances.
    private EmptyStatement() {
    }

    /**
     * Does totally nothing.
     *
     * @param t the exception to ignore.
     */
    public static void ignore(Throwable t) {
    }


}
