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

package com.hazelcast.client;

import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * User: sancar
 * Date: 8/16/13
 * Time: 11:42 AM
 */
public class DistributedObjectInfo implements Portable {

    private String serviceName;
    private Object id;

    DistributedObjectInfo(){

    }

    DistributedObjectInfo(String serviceName, Object id) {
        this.serviceName = serviceName;
        this.id = id;
    }

    @Override
    public int getFactoryId() {
        return ClientPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return ClientPortableHook.DISTRIBUTED_OBJECT_INFO;
    }

    public String getServiceName() {
        return serviceName;
    }

    public Object getId() {
        return id;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("sn", serviceName);
        final ObjectDataOutput rawDataOutput = writer.getRawDataOutput();
        rawDataOutput.writeObject(id);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        serviceName = reader.readUTF("sn");
        id = reader.getRawDataInput().readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DistributedObjectInfo that = (DistributedObjectInfo) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (serviceName != null ? !serviceName.equals(that.serviceName) : that.serviceName != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = serviceName != null ? serviceName.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        return result;
    }
}
