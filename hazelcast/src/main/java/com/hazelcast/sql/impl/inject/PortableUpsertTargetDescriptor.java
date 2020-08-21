/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.inject;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;

import java.io.IOException;
import java.util.Objects;

public class PortableUpsertTargetDescriptor implements UpsertTargetDescriptor, IdentifiedDataSerializable {

    private int portableFactoryId;
    private int portableClassId;
    private int portableClassVersion;

    @SuppressWarnings("unused")
    public PortableUpsertTargetDescriptor() {
    }

    public PortableUpsertTargetDescriptor(int portableFactoryId, int portableClassId, int portableClassVersion) {
        this.portableFactoryId = portableFactoryId;
        this.portableClassId = portableClassId;
        this.portableClassVersion = portableClassVersion;
    }

    public int getPortableFactoryId() {
        return portableFactoryId;
    }

    public int getPortableClassId() {
        return portableClassId;
    }

    public int getPortableClassVersion() {
        return portableClassVersion;
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.TARGET_DESCRIPTOR_PORTABLE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(portableFactoryId);
        out.writeInt(portableClassId);
        out.writeInt(portableClassVersion);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        portableFactoryId = in.readInt();
        portableClassId = in.readInt();
        portableClassVersion = in.readInt();
    }

    @Override
    public String toString() {
        return "PortableUpsertTargetDescriptor{"
                + "portableFactoryId=" + portableFactoryId
                + ", portableClassId=" + portableClassId
                + ", portableClassVersion=" + portableClassVersion
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PortableUpsertTargetDescriptor that = (PortableUpsertTargetDescriptor) o;
        return portableFactoryId == that.portableFactoryId
                && portableClassId == that.portableClassId
                && portableClassVersion == that.portableClassVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(portableFactoryId, portableClassId, portableClassVersion);
    }
}
