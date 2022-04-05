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

package com.hazelcast.internal.longregister.operations;

import com.hazelcast.internal.longregister.LongRegister;
import com.hazelcast.internal.longregister.LongRegisterDataSerializerHook;
import com.hazelcast.internal.longregister.LongRegisterService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.NamedOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import java.io.IOException;

public abstract class AbstractLongRegisterOperation extends Operation
        implements NamedOperation, PartitionAwareOperation, IdentifiedDataSerializable {

    protected String name;

    public AbstractLongRegisterOperation() {
    }

    public AbstractLongRegisterOperation(String name) {
        this.name = name;
    }

    public LongRegister getLongContainer() {
        LongRegisterService service = getService();
        return service.getLongRegister(name);
    }

    @Override
    public final String getServiceName() {
        return LongRegisterService.SERVICE_NAME;
    }

    @Override
    public final String getName() {
        return name;
    }

    @Override
    public final int getFactoryId() {
        return LongRegisterDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readString();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", name=").append(name);
    }
}
