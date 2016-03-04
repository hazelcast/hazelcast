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

package com.hazelcast.jet.impl.application.localization;

import com.hazelcast.jet.impl.application.LocalizationResourceDescriptor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;


public class Chunk implements DataSerializable {
    private byte[] bytes;
    private long length;
    private LocalizationResourceDescriptor descriptor;

    public Chunk() {

    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public Chunk(byte[] bytes, LocalizationResourceDescriptor descriptor, long length) {
        this.bytes = bytes;
        this.length = length;
        this.descriptor = descriptor;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public byte[] getBytes() {
        return bytes;
    }

    public LocalizationResourceDescriptor getDescriptor() {
        return descriptor;
    }

    public long getLength() {
        return length;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByteArray(bytes);
        out.writeObject(this.descriptor);
        out.writeLong(length);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        bytes = in.readByteArray();
        descriptor = in.readObject();
        length = in.readLong();
    }
}
