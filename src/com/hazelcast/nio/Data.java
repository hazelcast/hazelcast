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
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import com.hazelcast.impl.ThreadContext;

public final class Data implements DataSerializable {
    final List<ByteBuffer> lsData = new ArrayList<ByteBuffer>(12);

    int size = 0;

    ByteBuffer bbCurrentlyRead = null;

    int readSize = 0;

    int hash = Integer.MIN_VALUE;

    public Data() {
    }

    public void digest(MessageDigest md) {
        for (ByteBuffer bb : lsData) {
            md.update(bb.array(), 0, bb.limit());
        }
    }

    public int copyToBuffer(ByteBuffer to) {
        int written = 0;
        int len = lsData.size();
        for (int i = 0; i < len; i++) {
            ByteBuffer bb = lsData.get(i);
            int limit = bb.limit();
            if (limit == 0)
                throw new RuntimeException("limit cannot be zero");
            to.put(bb.array(), 0, limit);
            written += limit;
        }
        if (written != size)
            throw new RuntimeException("copyToBuffer didn't write all data. Written: " + written
                    + " size: " + size);
        return written;
    }

    public boolean shouldRead() {
        return (readSize < size);
    }

    public boolean add(ByteBuffer bb) {
        if (bb.position() == 0)
            throw new RuntimeException("Position cannot be 0");
        lsData.add(bb);
        size += bb.position();
        return true;
    }

    public void setNoData() {
        int len = lsData.size();
        for (int i = 0; i < len; i++) {
            ByteBuffer bb = lsData.get(i);
            bb.clear();
            ThreadContext.get().getBufferPool().release(bb);
        }
        lsData.clear();
        prepareForRead();
    }

    public void prepareForRead() {
        if (lsData.size() > 0)
            throw new RuntimeException("prepareForRead has data " + this);
        size = 0;
        readSize = 0;
        bbCurrentlyRead = null;
        hash = Integer.MIN_VALUE;
    }

    public void read(ByteBuffer src) {
        while (true) {
            int remaining = size - readSize;
            if (bbCurrentlyRead == null) {
                bbCurrentlyRead = ThreadContext.get().getBufferPool().obtain();
                bbCurrentlyRead.limit((remaining > 1024) ? 1024 : remaining);
            }
            readSize += BufferUtil.copy(src, bbCurrentlyRead);
            if (readSize >= size) {
                if (bbCurrentlyRead.position() == 0)
                    throw new RuntimeException("Position cannot be 0");
                lsData.add(bbCurrentlyRead);
                bbCurrentlyRead = null;
                return;
            }
            if (bbCurrentlyRead.remaining() == 0) {
                if (bbCurrentlyRead.position() == 0)
                    throw new RuntimeException("Position cannot be 0");
                lsData.add(bbCurrentlyRead);
                bbCurrentlyRead = null;
            }
            if (src.remaining() == 0)
                return;
        }
    }

    public void postRead() {
        int totalRead = 0;
        int len = lsData.size();
        for (int i = 0; i < len; i++) {
            ByteBuffer bb = lsData.get(i);
            totalRead += bb.position();
            if (i < (len - 1) && bb.position() != 1024) {
                throw new RuntimeException(len + " This buffer size has to be 1024. "
                        + bb.position() + "  index: " + i);
            }
            if (i == (len - 1) && bb.position() == 0) {
                throw new RuntimeException("Last buffer cannot be zero. " + bb.position());
            }
            bb.flip();
        }
        if (totalRead != size)
            throw new RuntimeException(totalRead + " but size should be " + size);
        bbCurrentlyRead = null;
        readSize = 0;
    }

    public boolean isEmpty() {
        return size == 0 && lsData.size() == 0;
    }

    public int size() {
        return size;
    }

    public void readData(DataInput in) throws IOException {
        int remaining = in.readInt();
        while (remaining > 0) {
            ByteBuffer bb = ThreadContext.get().getBufferPool().obtain();
            int sizeToRead = (remaining > 1024) ? 1024 : remaining;
            byte[] bytes = new byte[sizeToRead];
            in.readFully(bytes);
            bb.put(bytes);
            add(bb);
            remaining -= sizeToRead;
        }
        postRead();
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeInt(size);
        for (ByteBuffer bb : lsData) {
            out.write(bb.array(), 0, bb.limit());
        }
    }

    @Override
    public int hashCode() {
        if (hash == Integer.MIN_VALUE) {
            int h = 1;
            for (ByteBuffer bb : lsData) {
                int limit = bb.limit();
                byte[] buffer = bb.array();
                for (int i = 0; i < limit; i++) {
                    h = 31 * h + buffer[i];
                }
            }
            hash = h;
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (this == obj)
            return true;
        Data data = (Data) obj;
        if (data.size != size)
            return false;
        int bufferCount = lsData.size();
        if (bufferCount != data.lsData.size())
            return false;
        for (int i = 0; i < bufferCount; i++) {
            ByteBuffer thisBB = lsData.get(i);
            byte[] thisBuffer = thisBB.array();
            byte[] dataBuffer = data.lsData.get(i).array();
            int limit = thisBB.limit();
            for (int b = 0; b < limit; b++) {
                if (thisBuffer[b] != dataBuffer[b])
                    return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "Data ls.size=" + lsData.size() + " size = " + size;
    }
}