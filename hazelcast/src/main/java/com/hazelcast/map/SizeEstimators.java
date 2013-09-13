package com.hazelcast.map;

/**
 * Created with IntelliJ IDEA.
 * User: ahmet
 * Date: 08.09.2013
 * Time: 14:04
 */
public final class SizeEstimators {

    private SizeEstimators(){  }

    public static SizeEstimator createMapSizeEstimator( boolean statisticsEnabled )
    {
        final SizeEstimator mapSizeEstimator = statisticsEnabled ? new MapSizeEstimator() : EMPTY_SIZE_ESTIMATOR;

        return mapSizeEstimator;
    }

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
