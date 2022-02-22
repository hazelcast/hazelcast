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

package com.hazelcast.client.impl.operations;

import com.hazelcast.client.impl.ClientDataSerializerHook;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;

import java.io.IOException;
import java.util.UUID;

public final class OperationFactoryWrapper implements OperationFactory {

    private OperationFactory opFactory;
    private UUID uuid;

    public OperationFactoryWrapper() {
    }

    public OperationFactoryWrapper(OperationFactory opFactory, UUID uuid) {
        this.opFactory = opFactory;
        this.uuid = uuid;
    }

    @Override
    public Operation createOperation() {
        Operation op = opFactory.createOperation();
        op.setCallerUuid(uuid);
        return op;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        UUIDSerializationUtil.writeUUID(out, uuid);
        out.writeObject(opFactory);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        uuid = UUIDSerializationUtil.readUUID(in);
        opFactory = in.readObject();
    }

    public OperationFactory getOperationFactory() {
        return opFactory;
    }

    public UUID getUuid() {
        return uuid;
    }

    @Override
    public int getFactoryId() {
        return ClientDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClientDataSerializerHook.OP_FACTORY_WRAPPER;
    }

    @Override
    public String toString() {
        return "OperationFactoryWrapper{"
                + "opFactory=" + opFactory
                + ", uuid='" + uuid + '\''
                + '}';
    }
}
