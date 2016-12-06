/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.distributedclassloading.impl.operation;

import com.hazelcast.internal.distributedclassloading.impl.ClassData;
import com.hazelcast.internal.distributedclassloading.DistributedClassloadingService;
import com.hazelcast.internal.distributedclassloading.impl.ClassloadingSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.UrgentSystemOperation;

import java.io.IOException;

public final class ClassDataFinderOperation extends Operation implements UrgentSystemOperation, IdentifiedDataSerializable {

    private String className;
    private ClassData response;

    public ClassDataFinderOperation(String className) {
        this.className = className;
    }

    public ClassDataFinderOperation() {
    }

    @Override
    public ClassData getResponse() {
        return response;
    }

    @Override
    public void run() throws Exception {
        DistributedClassloadingService service = getService();
        response = service.getClassDataOrNull(className);
    }

    @Override
    public String getServiceName() {
        return DistributedClassloadingService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(className);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        className = in.readUTF();
    }

    @Override
    public int getFactoryId() {
        return ClassloadingSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClassloadingSerializerHook.CLASS_DATA_FINDER_OP;
    }
}
