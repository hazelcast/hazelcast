package com.hazelcast.query.extractor

/**
 * Exception thrown if there is any checked or unchecked exception caught in the value reading in {@link ValueReader}
 */
public class ValueReadingException extends RuntimeException {

    public ValueReadingException(String message, Throwable throwable) {
        super(message, throwable)
    }

}
