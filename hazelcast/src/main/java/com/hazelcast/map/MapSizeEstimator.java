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

import com.hazelcast.map.record.ObjectRecord;
import com.hazelcast.map.record.Record;

/**
 * User: ahmet
 * Date: 06.09.2013
 */
class MapSizeEstimator<T extends Record> implements SizeEstimator<T> {

    private volatile long _size;

    public long getSize() {
        return _size;
    }

    public void add(long size) {
        _size += size;
    }

    public void reset() {
        _size = 0;
    }

    public long getCost(T record) {
        if (record == null) {
            return 0L;
        }
        if (record instanceof ObjectRecord) {
            return 0L;
        }
        // entry size in CHM
        long refSize = 4 * ((Integer.SIZE / Byte.SIZE));
        final long valueSize = record.getCost();
        return refSize + valueSize;
    }
}
