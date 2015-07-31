/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.map.impl.MapKeySet;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.NearCacheProvider;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import java.io.IOException;
import java.util.Set;

public class NearCacheKeySetInvalidationOperation extends AbstractOperation implements MutatingOperation {
    private MapService mapService;
    private MapKeySet mapKeySet;
    private String mapName;

    public NearCacheKeySetInvalidationOperation() {
    }

    public NearCacheKeySetInvalidationOperation(String mapName, Set<Data> keys) {
        this.mapKeySet = new MapKeySet(keys);
        this.mapName = mapName;
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public void run() {
        mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        if (mapServiceContext.getMapContainer(mapName).isNearCacheEnabled()) {
            NearCacheProvider nearCacheProvider = mapServiceContext.getNearCacheProvider();
            nearCacheProvider.invalidateNearCache(mapName, mapKeySet.getKeySet());
        } else {
            getLogger().warning("Cache clear operation has been accepted while near cache is not enabled for "
                    + mapName + " map. Possible configuration conflict among nodes.");
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(mapName);
        mapKeySet.writeData(out);
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapName = in.readUTF();
        mapKeySet = new MapKeySet();
        mapKeySet.readData(in);
    }

    @Override
    public String toString() {
        return "NearCacheKeySetInvalidationOperation{}";
    }
}
