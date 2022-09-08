package com.hazelcast.sql.impl;

/**
 * Exception marking the query as completed even if the inbound edges are still producing values.
 *
 * Needed for streaming jobs, where {@link com.hazelcast.jet.core.Processor#complete} will never be called
 * if inbound edges are too fast. Should be always ignored on client side, it just means "no more data, but it's ok".
 */
public class QueryEndException extends RuntimeException {

    public QueryEndException() {
        // Use writableStackTrace = false, the exception is not created at a place where it's thrown,
        // it's better if it has no stack trace then.
        super("Done by reaching the end specified by the query", null, false, false);
    }
}
