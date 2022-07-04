/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.AbstractNamedOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;

public class RemoveInterceptorOperation extends AbstractNamedOperation
        implements MutatingOperation {

    private String id;
    private boolean interceptorRemoved;

    public RemoveInterceptorOperation() {
    }

    public RemoveInterceptorOperation(String mapName, String id) {
        super(mapName);
        this.id = id;
    }

    @Override
    public void run() {
        interceptorRemoved = getMapContainer().getInterceptorRegistry().deregister(id);
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    private MapContainer getMapContainer() {
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getMapContainer(name);
    }

    @Override
    public Object getResponse() {
        return interceptorRemoved;
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        id = in.readString();
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(id);
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", id=").append(id);
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.REMOVE_INTERCEPTOR;
    }
}
