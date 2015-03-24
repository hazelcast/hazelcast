package com.hazelcast.spi.persistence;

/**
 * @author mdogan 09/02/15
 */
public interface DBIterator {

    boolean hasNext();

    void next();

    byte[] key();

    byte[] value();

    void close();

}
