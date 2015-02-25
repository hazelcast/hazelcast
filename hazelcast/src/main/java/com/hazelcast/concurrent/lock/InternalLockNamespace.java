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

package com.hazelcast.concurrent.lock;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.ObjectNamespace;

import java.io.IOException;

public final class InternalLockNamespace implements ObjectNamespace {

    private String name;

    public InternalLockNamespace() {
    }

    public InternalLockNamespace(String name) {
        this.name = name;
    }

    @Override
    public String getServiceName() {
        return LockService.SERVICE_NAME;
    }

    @Override
    public String getObjectName() {
        return name;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        //todo: we don't include the name in the equals?
        return true;
    }

    @Override
    public int hashCode() {
        //todo: we don't include the name in the hash?
        return getServiceName().hashCode();
    }
}
