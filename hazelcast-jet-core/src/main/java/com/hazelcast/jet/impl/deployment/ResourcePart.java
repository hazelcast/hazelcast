/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.deployment;

import com.hazelcast.jet.JetDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;


public class ResourcePart implements IdentifiedDataSerializable {
    private byte[] bytes;
    private ResourceDescriptor descriptor;
    private int offset;

    public ResourcePart() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public ResourcePart(ResourceDescriptor descriptor, byte[] bytes, int offset) {
        this.bytes = bytes;
        this.descriptor = descriptor;
        this.offset = offset;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public byte[] getBytes() {
        return bytes;
    }

    public ResourceDescriptor getDescriptor() {
        return descriptor;
    }

    public int getOffset() {
        return offset;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(this.descriptor);
        out.writeByteArray(bytes);
        out.writeInt(offset);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        descriptor = in.readObject();
        bytes = in.readByteArray();
        offset = in.readInt();
    }

    @Override
    public int getFactoryId() {
        return JetDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetDataSerializerHook.RESOURCE_PART;
    }

    @Override
    public String toString() {
        return "Chunk{"
                + "length=" + bytes.length
                + ", descriptor=" + descriptor
                + ", offset=" + offset
                + '}';
    }

}
