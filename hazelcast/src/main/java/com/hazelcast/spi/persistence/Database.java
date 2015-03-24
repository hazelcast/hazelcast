package com.hazelcast.spi.persistence;

/**
 * @author mdogan 06/01/15
 */
public interface Database {

    void put(byte[] key, byte[] value) throws DBException;

    byte[] get(byte[] key) throws DBException;

    int get(byte[] key, byte[] value) throws DBException;

    void remove(byte[] key) throws DBException;

    DBIterator iterator();

    boolean isEmpty();

    /**
     * not thread-safe
     * @throws DBException
     */
    void clear() throws DBException;

    /**
     * not thread-safe
     * @throws DBException
     */
    void close() throws DBException;

    /**
     * not thread-safe
     * @throws DBException
     */
    void destroy() throws DBException;

    String name();
}
