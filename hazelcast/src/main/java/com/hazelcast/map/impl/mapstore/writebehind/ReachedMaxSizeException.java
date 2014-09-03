package com.hazelcast.map.impl.mapstore.writebehind;

/**
 * Exception thrown when a {@link com.hazelcast.map.impl.mapstore.writebehind.BoundedArrayWriteBehindQueue}
 * rejects to accept an offer.
 */
public class ReachedMaxSizeException extends RuntimeException {

    private static final long serialVersionUID = -2352370861668557606L;

    public ReachedMaxSizeException(String msg) {
        super(msg);
    }
}
