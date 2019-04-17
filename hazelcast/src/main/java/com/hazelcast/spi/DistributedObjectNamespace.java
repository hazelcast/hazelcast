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

package com.hazelcast.spi;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.SpiDataSerializerHook;

import java.io.IOException;
import java.util.Objects;

/**
 * Default {@link com.hazelcast.spi.ObjectNamespace} implementation for {@link com.hazelcast.core.DistributedObject}.
 *
 * @since 3.9
 */
public final class DistributedObjectNamespace implements ObjectNamespace, IdentifiedDataSerializable {

    private String service;
    private String objectName;

    public DistributedObjectNamespace() {
    }

    public DistributedObjectNamespace(String serviceName, String objectName) {
        this.service = serviceName;
        this.objectName = objectName;
    }

    public DistributedObjectNamespace(ObjectNamespace namespace) {
        this(namespace.getServiceName(), namespace.getObjectName());
    }

    @Override
    public String getServiceName() {
        return service;
    }

    @Override
    public String getObjectName() {
        return objectName;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(service);
        out.writeUTF(objectName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        service = in.readUTF();
        objectName = in.readUTF();
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SpiDataSerializerHook.DISTRIBUTED_OBJECT_NS;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DistributedObjectNamespace)) {
            return false;
        }

        DistributedObjectNamespace that = (DistributedObjectNamespace) o;

        if (!Objects.equals(service, that.service)) {
            return false;
        }
        return Objects.equals(objectName, that.objectName);

    }

    @Override
    public int hashCode() {
        int result = service != null ? service.hashCode() : 0;
        result = 31 * result + (objectName != null ? objectName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "DistributedObjectNamespace{" + "service='" + service + '\'' + ", objectName='" + objectName + '\'' + '}';
    }
}
