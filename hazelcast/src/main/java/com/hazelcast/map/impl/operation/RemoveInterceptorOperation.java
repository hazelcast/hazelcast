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

import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import java.io.IOException;

public class RemoveInterceptorOperation extends AbstractOperation {

    private MapService mapService;
    private String mapName;
    private String id;

    public RemoveInterceptorOperation() {
    }

    public RemoveInterceptorOperation(String mapName, String id) {
        this.mapName = mapName;
        this.id = id;
    }

    @Override
    public void run() {
        mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        mapContainer.removeInterceptor(id);
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapName = in.readUTF();
        id = in.readUTF();
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(mapName);
        out.writeUTF(id);
    }

    @Override
    public String toString() {
        return "RemoveInterceptorOperation{}";
    }
}
