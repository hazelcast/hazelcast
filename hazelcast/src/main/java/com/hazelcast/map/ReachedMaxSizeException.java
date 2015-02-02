package com.hazelcast.map;

/**
 * Exception thrown when a write-behind {@link com.hazelcast.core.MapStore} rejects to accept a new element.
 * Used when {@link com.hazelcast.config.MapStoreConfig#writeCoalescing} is set to true.
 */
public class ReachedMaxSizeException extends RuntimeException {

    private static final long serialVersionUID = -2352370861668557606L;

    public ReachedMaxSizeException(String msg) {
        super(msg);
    }
}
