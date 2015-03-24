package com.hazelcast.spi.persistence;

import com.hazelcast.core.HazelcastException;

/**
 * @author mdogan 06/01/15
 */
public class DBException extends HazelcastException {

    public DBException() {
    }

    public DBException(String message) {
        super(message);
    }

    public DBException(String message, Throwable cause) {
        super(message, cause);
    }

    public DBException(Throwable cause) {
        super(cause);
    }
}

