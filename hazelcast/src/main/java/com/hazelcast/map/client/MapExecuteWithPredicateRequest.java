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

package com.hazelcast.map.client;

import com.hazelcast.client.AllPartitionsClientRequest;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapEntrySet;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.map.operation.PartitionWideEntryWithPredicateOperationFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicate;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.OperationFactory;
import java.io.IOException;
import java.security.Permission;
import java.util.Map;
import java.util.Set;

public class MapExecuteWithPredicateRequest extends AllPartitionsClientRequest {
    private String name;
    private EntryProcessor processor;
    private Predicate predicate;

    public MapExecuteWithPredicateRequest() {
    }

    public MapExecuteWithPredicateRequest(String name, EntryProcessor processor, Predicate predicate) {
        this.name = name;
        this.processor = processor;
        this.predicate = predicate;
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new PartitionWideEntryWithPredicateOperationFactory(name, processor, predicate);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        MapEntrySet result = new MapEntrySet();
        MapService mapService = getService();
        for (Object o : map.values()) {
            if (o != null) {
                MapEntrySet entrySet = (MapEntrySet) mapService.toObject(o);
                Set<Map.Entry<Data, Data>> entries = entrySet.getEntrySet();
                for (Map.Entry<Data, Data> entry : entries) {
                    result.add(entry);
                }
            }
        }
        return result;
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.EXECUTE_WITH_PREDICATE;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeObject(processor);
        out.writeObject(predicate);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        final ObjectDataInput in = reader.getRawDataInput();
        processor = in.readObject();
        predicate = in.readObject();
    }

    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_PUT, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getMethodName() {
        return "executeOnEntries";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{processor, predicate};
    }
}
