/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
 */
class MapSizeEstimator extends AbstractSizeEstimator {

    protected MapSizeEstimator(){
        super();
    }


    @Override
    public <T> long getCost(T record) {

        // immediate check nothing to do if record is null
        if( record == null ){
            return 0;
        }

        if( record instanceof NearCache.CacheRecord)
        {
            final NearCache.CacheRecord rec = (NearCache.CacheRecord)record;
            final long cost = rec.getCost();
            // if  cost is zero, type of cached object is not Data.
            if( cost == 0 ) return 0;

            long size = 0;
            // key ref. size in map.
            size += ((Integer.SIZE/Byte.SIZE));
            size += rec.getCost() ;
            return size;
        }
        else if( record instanceof DataRecord)
        {
            final Record rec = (Record)record;
            long size = 0;
            // key ref. size in map.
            size += ((Integer.SIZE/Byte.SIZE));
            size += rec.getCost() ;
            return size;
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
