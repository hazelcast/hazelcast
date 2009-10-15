/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.nio;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public final class Data implements DataSerializable {

    private static final long serialVersionUID = 5382795596398809726L;

    public ByteBuffer buffer = null;

    public int size = 0;

    public int hash = Integer.MIN_VALUE;

    public long createDate = -1;

    public Data() {
    }

    public Data(int size) {
        this.size = size;
        if (size > 0) {
            this.buffer = ByteBuffer.allocate(size);
        }
    }

    public Data(byte[] bytes, int size) {
        this.size = size;
        if (size > 0) {
            this.buffer = ByteBuffer.allocate(size);
            System.arraycopy(bytes, 0, buffer.array(), 0, size);
        }
    }

    public Data(ByteBuffer bb) {
        this.size = bb.array().length;
        this.buffer = bb;
    }

    public Data(Data data) {
        this.size = data.size();
        this.buffer = ByteBuffer.allocate(size);
        System.arraycopy(data.buffer.array(), 0, buffer.array(), 0, size);
        this.hash = data.hash;
    }

    public boolean shouldRead() {
        return (size > 0 && buffer != null && buffer.hasRemaining());
    }

    public void read(ByteBuffer src) {
        IOUtil.copyToHeapBuffer(src, buffer);
    }

    public void postRead() {
        if (buffer != null) {
            buffer.flip();
        }
    }

    public int size() {
        return size;
    }

    public void readData(DataInput in) throws IOException {
        this.size = in.readInt();
        if (size > 0) {
            byte[] bytes = new byte[size];
            in.readFully(bytes);
            this.buffer = ByteBuffer.wrap(bytes);
            postRead();
        }
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeInt(size);
        out.write(buffer.array());
    }

    @Override
    public int hashCode() {
        if (buffer == null) return Integer.MIN_VALUE;
        if (hash == Integer.MIN_VALUE) {
            int h = 1;
            byte[] b = buffer.array();
            for (int i = 0; i < b.length; i++) {
                h = 31 * h + b[i];
            }
            hash = h;
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if ((obj == null) || !(obj instanceof Data))
            return false;
        if (this == obj)
            return true;
        Data data = (Data) obj;
        if (data.size != size)
            return false;
        if (size == 0) return (data.buffer == null);
        return Arrays.equals(data.buffer.array(), buffer.array());
    }

    @Override
    public String toString() {
        return "Data size = " + size;
    }
}