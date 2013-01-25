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

package com.hazelcast.nio.serialization;

import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.UTFUtil;

import java.io.*;

/**
 * @mdogan 01/23/13
 */
public class ObjectDataOutputStream extends OutputStream implements ObjectDataOutput, Closeable, SerializationContextAware {

    private final SerializationService serializationService;
    private final DataOutputStream dataOut;

    public ObjectDataOutputStream(OutputStream outputStream, SerializationService serializationService) {
        this.serializationService = serializationService;
        this.dataOut = new DataOutputStream(outputStream);
    }

    public void write(int b) throws IOException {
        dataOut.write(b);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        dataOut.write(b, off, len);
    }

    public void writeBoolean(boolean v) throws IOException {
        dataOut.writeBoolean(v);
    }

    public void writeByte(int v) throws IOException {
        dataOut.writeByte(v);
    }

    public void writeShort(int v) throws IOException {
        dataOut.writeShort(v);
    }

    public void writeChar(int v) throws IOException {
        dataOut.writeChar(v);
    }

    public void writeInt(int v) throws IOException {
        dataOut.writeInt(v);
    }

    public void writeLong(long v) throws IOException {
        dataOut.writeLong(v);
    }

    public void writeFloat(float v) throws IOException {
        dataOut.writeFloat(v);
    }

    public void writeDouble(double v) throws IOException {
        dataOut.writeDouble(v);
    }

    public void writeBytes(String s) throws IOException {
        dataOut.writeBytes(s);
    }

    public void writeChars(String s) throws IOException {
        dataOut.writeChars(s);
    }

    public void writeUTF(String str) throws IOException {
//        dataOut.writeUTF(str);
        UTFUtil.writeUTF(this, str);
    }

    public void write(byte[] b) throws IOException {
        dataOut.write(b);
    }

    public void writeObject(Object object) throws IOException {
        serializationService.writeObject(this, object);
    }

    public byte[] toByteArray() {
        throw new UnsupportedOperationException();
    }

    public void flush() throws IOException {
        dataOut.flush();
    }

    public void close() throws IOException {
        dataOut.close();
    }

    public SerializationContext getSerializationContext() {
        return serializationService.getSerializationContext();
    }
}
