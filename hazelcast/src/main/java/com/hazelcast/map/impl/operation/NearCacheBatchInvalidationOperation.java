/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.nearcache.AbstractNearCacheInvalidator;
import com.hazelcast.map.impl.nearcache.NearCacheInvalidator;
import com.hazelcast.map.impl.nearcache.NearCacheProvider;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class NearCacheBatchInvalidationOperation extends MapOperation implements MutatingOperation {

    private List<Data> keys;

    public NearCacheBatchInvalidationOperation() {
    }

    public NearCacheBatchInvalidationOperation(String mapName, List<Data> keys) {
        super(mapName);
        this.keys = checkNotNull(keys);
    }

    @Override
    public void run() {
        if (mapContainer.hasMemberNearCache()) {
            NearCacheProvider nearCacheProvider = mapServiceContext.getNearCacheProvider();
            NearCacheInvalidator nearCacheInvalidator = nearCacheProvider.getNearCacheInvalidator();
            ((AbstractNearCacheInvalidator) nearCacheInvalidator).invalidateLocal(name, null, keys);
        } else {
            getLogger().warning("Cache clear operation has been accepted while near cache is not enabled for "
                    + name + " map. Possible configuration conflict among nodes.");
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(keys.size());
        for (Data key : keys) {
            out.writeData(key);
        }
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        List<Data> keys = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            Data key = in.readData();
            keys.add(key);
        }
        this.keys = keys;
    }
}
