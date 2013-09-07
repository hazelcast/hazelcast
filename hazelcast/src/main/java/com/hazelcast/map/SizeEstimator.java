package com.hazelcast.map;

/**
 * Created with IntelliJ IDEA.
 * User: ahmet
 * Date: 06.09.2013
 * Time: 08:10
 * To change this template use File | Settings | File Templates.
 */
public interface SizeEstimator {

    long getSize();

    void add( long size );

    <T> long getCost( T record );

    void reset();

    static final SizeEstimator EMPTY_SIZE_ESTIMATOR = new SizeEstimator(){

        @Override
        public long getSize() {
            //not implemented
            return 0;
        }

        @Override
        public void add(long size) {
            //not implemented
        }

        @Override
        public <T> long getCost(T record) {
            //not implemented
            return 0;
        }

        @Override
        public void reset() {
            //not implemented
        }
    };

}
