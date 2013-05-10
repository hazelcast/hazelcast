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

package com.hazelcast.map.clientv2;

import com.hazelcast.clientv2.AllPartitionsClientRequest;
import com.hazelcast.clientv2.MultiTargetClientRequest;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.util.QueryResultStream;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class MapQueryRequest extends MultiTargetClientRequest {

    private String name;
    private Predicate predicate;
    private QueryResultStream.IterationType iterationType;

    public MapQueryRequest() {
    }

    public MapQueryRequest(String name, Predicate predicate, QueryResultStream.IterationType iterationType) {
        this.name = name;
        this.predicate = predicate;
        this.iterationType = iterationType;
    }

    @Override
    protected OperationFactory createOperationFactory() {
        // todo implement
        return null;
    }

    @Override
    protected Object reduce(Map<Address, Object> map) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Collection<Address> getTargets() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.ENTRY_SET_QUERY;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeUTF("t", iterationType.name());
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeObject(predicate);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        iterationType = QueryResultStream.IterationType.valueOf(reader.readUTF("t"));
        final ObjectDataInput in = reader.getRawDataInput();
        predicate = in.readObject();
    }
}
