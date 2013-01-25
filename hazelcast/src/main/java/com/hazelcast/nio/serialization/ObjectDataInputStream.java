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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.UTFUtil;

import java.io.*;

/**
 * @mdogan 1/23/13
 */
public class ObjectDataInputStream extends InputStream implements ObjectDataInput, Closeable, SerializationContextAware {

    private final SerializationService serializationService;
    private final DataInputStream dataInput;

    public ObjectDataInputStream(InputStream in, SerializationService serializationService) {
        this.serializationService = serializationService;
        this.dataInput = new DataInputStream(in);
    }

    public int read() throws IOException {
        return readByte();
    }

    @Override
    public long skip(long n) throws IOException {
        return dataInput.skip(n);
    }

    @Override
    public int available() throws IOException {
        return dataInput.available();
    }

    public int read(byte[] b) throws IOException {
        return dataInput.read(b);
    }

    public int read(byte[] b, int off, int len) throws IOException {
        return dataInput.read(b, off, len);
    }

    public void readFully(byte[] b) throws IOException {
        dataInput.readFully(b);
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        dataInput.readFully(b, off, len);
    }

    public int skipBytes(int n) throws IOException {
        return dataInput.skipBytes(n);
    }

    public boolean readBoolean() throws IOException {
        return dataInput.readBoolean();
    }

    public byte readByte() throws IOException {
        return dataInput.readByte();
    }

    public int readUnsignedByte() throws IOException {
        return dataInput.readUnsignedByte();
    }

    public short readShort() throws IOException {
        return dataInput.readShort();
    }

    public int readUnsignedShort() throws IOException {
        return dataInput.readUnsignedShort();
    }

    public char readChar() throws IOException {
        return dataInput.readChar();
    }

    public int readInt() throws IOException {
        return dataInput.readInt();
    }

    public long readLong() throws IOException {
        return dataInput.readLong();
    }

    public float readFloat() throws IOException {
        return dataInput.readFloat();
    }

    public double readDouble() throws IOException {
        return dataInput.readDouble();
    }

    @Deprecated
    public String readLine() throws IOException {
        return dataInput.readLine();
    }

    public String readUTF() throws IOException {
//        return dataInput.readUTF();
        return UTFUtil.readUTF(this);
    }

    public void close() throws IOException {
        dataInput.close();
    }

    public void mark(int readlimit) {
        dataInput.mark(readlimit);
    }

    public void reset() throws IOException {
        dataInput.reset();
    }

    public boolean markSupported() {
        return dataInput.markSupported();
    }

    public Object readObject() throws IOException {
        return serializationService.readObject(this);
    }

    public SerializationContext getSerializationContext() {
        return serializationService.getSerializationContext();
    }
}
