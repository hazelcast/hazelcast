/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization;

import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.annotation.PrivateApi;

import java.io.IOException;
import java.util.Objects;

import static com.hazelcast.internal.serialization.SerializableByConvention.Reason.PUBLIC_API;

/**
 * Uniquely defines a {@link Portable} class.
 *
 * @see ClassDefinition#getPortableId()
 * @see FieldDefinition#getPortableId()
 * @see ClassDefinitionBuilder#ClassDefinitionBuilder(PortableId) new ClassDefinitionBuilder(PortableId)
 *
 * @since 5.4
 */
@SerializableByConvention(PUBLIC_API)
public class PortableId implements DataSerializable {
    private int factoryId;
    private int classId;
    private int version;

    @PrivateApi
    public PortableId() { }

    public PortableId(int factoryId, int classId, int version) {
        this.factoryId = factoryId;
        this.classId = classId;
        this.version = version;
    }

    public PortableId(String portableId) {
        String[] components = portableId.split(":");
        assert components.length == 3 : "Number of Portable ID components should always be 3";

        this.factoryId = Integer.parseInt(components[0]);
        this.classId = Integer.parseInt(components[1]);
        this.version = Integer.parseInt(components[2]);
    }

    public int getFactoryId() {
        return factoryId;
    }

    public int getClassId() {
        return classId;
    }

    public int getVersion() {
        return version;
    }

    @PrivateApi
    public void setVersionIfNotSet(int version) {
        if (this.version < 0) {
            this.version = version;
        }
    }

    @Override
    public String toString() {
        return factoryId + ":" + classId + ":" + version;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(factoryId);
        out.writeInt(classId);
        out.writeInt(version);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        factoryId = in.readInt();
        classId = in.readInt();
        version = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PortableId that = (PortableId) o;
        return factoryId == that.factoryId
                && classId == that.classId
                && version == that.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(factoryId, classId, version);
    }
}
