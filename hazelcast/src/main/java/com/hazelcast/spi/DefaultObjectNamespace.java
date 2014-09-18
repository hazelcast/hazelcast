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

package com.hazelcast.spi;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Default {@link com.hazelcast.spi.ObjectNamespace} implementation.
 */
public final class DefaultObjectNamespace implements ObjectNamespace {

    private String service;

    private String objectName;

    public DefaultObjectNamespace() {
    }

    public DefaultObjectNamespace(String serviceName, String objectName) {
        this.service = serviceName;
        this.objectName = objectName;
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DefaultObjectNamespace that = (DefaultObjectNamespace) o;

        if (objectName != null ? !objectName.equals(that.objectName) : that.objectName != null) {
            return false;
        }
        if (service != null ? !service.equals(that.service) : that.service != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = service != null ? service.hashCode() : 0;
        result = 31 * result + (objectName != null ? objectName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("DefaultObjectNamespace");
        sb.append("{service='").append(service).append('\'');
        sb.append(", objectName=").append(objectName);
        sb.append('}');
        return sb.toString();
    }
}
