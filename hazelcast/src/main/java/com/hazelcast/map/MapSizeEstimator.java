package com.hazelcast.map;

import com.hazelcast.map.record.Record;
import com.hazelcast.util.ExceptionUtil;

/**
 * Created with IntelliJ IDEA.
 * User: ahmet
 * Date: 06.09.2013
 * Time: 07:51
 * To change this template use File | Settings | File Templates.
 */
public class MapSizeEstimator extends AbstractSizeEstimator {

    private MapSizeEstimator(){
        super();
    }

    public  static SizeEstimator createNew( boolean enabled ){

        final SizeEstimator mapSizeEstimator = enabled ? new MapSizeEstimator() : EMPTY_SIZE_ESTIMATOR;

        return mapSizeEstimator;
    }



    @Override
    public <T> long getCost(T record) {

        if( record instanceof NearCache.CacheRecord)
        {
            final NearCache.CacheRecord rec = (NearCache.CacheRecord)record;

            final long keySize = rec.getKey().totalSize();

            final long valueSize = rec.getCost() ;

            return keySize + valueSize;
        }
        else if( record instanceof  Record )
        {
            final Record rec = (Record)record;

            final long keySize = rec.getKey().totalSize();

            final long valueSize = rec.getCost() ;

            return keySize + valueSize;
        }


        final String msg =  "MapSizeEstimator::not known object for cost" +
                " calculation of a map [" + record.getClass().getCanonicalName()+"]";

        throw new RuntimeException( msg ) ;
    }
}
