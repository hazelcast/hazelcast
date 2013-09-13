package com.hazelcast.map;

/**
 * Created with IntelliJ IDEA.
 * User: ahmet
 * Date: 06.09.2013
 * Time: 08:10
 */
public interface SizeEstimator {

    long getSize();

    void add( long size );

    <T> long getCost( T record );

    void reset();
}
