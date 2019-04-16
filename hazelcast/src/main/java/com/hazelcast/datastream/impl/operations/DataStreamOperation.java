/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream.impl.operations;

import com.hazelcast.datastream.impl.DSContainer;
import com.hazelcast.datastream.impl.DSDataSerializerHook;
import com.hazelcast.datastream.impl.DSPartition;
import com.hazelcast.datastream.impl.DSService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.NamedOperation;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public abstract class DataStreamOperation extends Operation
        implements IdentifiedDataSerializable, NamedOperation {

    protected DSService service;
    protected DSContainer container;
    protected DSPartition partition;
    protected String name;

    public DataStreamOperation() {
    }

    public DataStreamOperation(String name) {
        this.name = name;
    }

    @Override
    public void beforeRun() throws Exception {
        super.beforeRun();
        service = getService();
        container = service.getDataStreamContainer(name);
        partition = container.getPartition(getPartitionId());
        partition.deleteRetiredRegions();
    }

    @Override
    public String getServiceName() {
        return DSService.SERVICE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getFactoryId() {
        return DSDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", name=").append(name);
    }
}
