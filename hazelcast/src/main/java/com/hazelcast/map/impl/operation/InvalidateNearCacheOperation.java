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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;
import java.io.IOException;

public class InvalidateNearCacheOperation extends AbstractOperation {

    private Data key;
    private String mapName;

    public InvalidateNearCacheOperation(String mapName, Data key) {
        this.key = key;
        this.mapName = mapName;
    }

    public InvalidateNearCacheOperation() {
    }

    public void run() {
        MapService mapService = getService();
        if (mapService.getMapServiceContext().getMapContainer(mapName).isNearCacheEnabled()) {
            mapService.getMapServiceContext().getNearCacheProvider().invalidateNearCache(mapName, key);
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
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapName = in.readUTF();
        key = in.readObject();
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(mapName);
        out.writeObject(key);
    }

    @Override
    public String toString() {
        return "InvalidateNearCacheOperation{}";
    }
}
