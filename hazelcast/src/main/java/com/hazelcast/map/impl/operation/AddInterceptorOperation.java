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

import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import java.io.IOException;

public class AddInterceptorOperation extends AbstractOperation {

    private MapService mapService;
    private String id;
    private MapInterceptor mapInterceptor;
    private String mapName;

    public AddInterceptorOperation() {
    }

    public AddInterceptorOperation(String id, MapInterceptor mapInterceptor, String mapName) {
        this.id = id;
        this.mapInterceptor = mapInterceptor;
        this.mapName = mapName;
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public void run() {
        mapService = getService();
        MapContainer mapContainer = mapService.getMapServiceContext().getMapContainer(mapName);
        mapContainer.addInterceptor(id, mapInterceptor);
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
        mapInterceptor = in.readObject();
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(mapName);
        out.writeUTF(id);
        out.writeObject(mapInterceptor);
    }

    @Override
    public String toString() {
        return "AddInterceptorOperation{}";
    }

}
