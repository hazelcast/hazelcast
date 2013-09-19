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


import java.util.concurrent.atomic.AtomicLong;

/**
 * Created with IntelliJ IDEA.
 * User: ahmet
 * Date: 06.09.2013
 * <p/>
 * Time: 07:51
 */
class NearCacheSizeEstimator implements SizeEstimator {

    private final AtomicLong _size = new AtomicLong(0L);

    protected NearCacheSizeEstimator() {
        super();
    }


    @Override
    public <T> long getCost(T record) {

        // immediate check nothing to do if record is null
        if (record == null) {
            return 0;
        }

        if (record instanceof NearCache.CacheRecord) {
            final NearCache.CacheRecord rec = (NearCache.CacheRecord) record;
            final long cost = rec.getCost();
            // if  cost is zero, type of cached object is not Data.
            // then omit.
            if (cost == 0) return 0;

            long size = 0;
            // key ref. size in map.
            size += 4 * ((Integer.SIZE / Byte.SIZE));
            size += rec.getCost();
            return size;
        }

        final String msg = "NearCacheSizeEstimator::not known object for near cache heap cost" +
                " calculation [" + record.getClass().getCanonicalName() + "]";

        throw new RuntimeException(msg);
    }

    @Override
    public long getSize() {
        return _size.longValue();
    }

    public void add(long size) {
        _size.addAndGet(size);
    }


    public void reset() {
        _size.set(0L);
    }


}
