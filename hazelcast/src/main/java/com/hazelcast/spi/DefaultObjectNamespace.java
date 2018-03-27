/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.SerializableByConvention;

import java.io.IOException;

import static com.hazelcast.nio.serialization.SerializableByConvention.Reason.PUBLIC_API;

/**
 * Default {@link ObjectNamespace} implementation.
 * @deprecated please use {@link DistributedObjectNamespace}
 */
@SerializableByConvention(PUBLIC_API)
@Deprecated
public class DefaultObjectNamespace implements ObjectNamespace {

    protected String service;

    protected String objectName;

    public DefaultObjectNamespace() {
    }

    public DefaultObjectNamespace(String serviceName, String objectName) {
        this.service = serviceName;
        this.objectName = objectName;
    }

    public DefaultObjectNamespace(ObjectNamespace namespace) {
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
        // writing as object for backward-compatibility
        out.writeObject(objectName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        service = in.readUTF();
        objectName = in.readObject();
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        // allow subclasses
        if (!(o instanceof DefaultObjectNamespace)) {
            return false;
        }

        DefaultObjectNamespace that = (DefaultObjectNamespace) o;
        return service.equals(that.service) && objectName.equals(that.objectName);
    }

    @Override
    public final int hashCode() {
        int result = service.hashCode();
        result = 31 * result + objectName.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "DefaultObjectNamespace{service='" + service + '\'' + ", objectName=" + objectName + '}';
    }
}
