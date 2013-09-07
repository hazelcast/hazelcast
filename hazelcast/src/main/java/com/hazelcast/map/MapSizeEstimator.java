package com.hazelcast.map;

import com.hazelcast.map.record.DataRecord;
import com.hazelcast.map.record.ObjectRecord;
import com.hazelcast.map.record.Record;

/**
 * Created with IntelliJ IDEA.
 * User: ahmet
 * Date: 06.09.2013
 *
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
        else if( record instanceof DataRecord)
        {
            final Record rec = (Record)record;

            final long keySize = rec.getKey().totalSize();

            final long valueSize = rec.getCost() ;

            return keySize + valueSize;
        }
        else if( record instanceof ObjectRecord)
        {
            // todo calculate object size properly.
            // calculating object size is omitted for now.
            return 0;
        }


        final String msg =  "MapSizeEstimator::not known object for map heap cost" +
                " calculation [" + record.getClass().getCanonicalName()+"]";

        throw new RuntimeException( msg ) ;
    }
}
