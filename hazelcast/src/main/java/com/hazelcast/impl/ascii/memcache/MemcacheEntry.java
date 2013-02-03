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

package com.hazelcast.impl.ascii.memcache;

import com.hazelcast.impl.ascii.TextCommandConstants;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public class MemcacheEntry implements DataSerializable, TextCommandConstants {
    byte[] bytes;
    int flag;

    public MemcacheEntry(String key, byte[] value, int flag) {
        byte[] flagBytes = new String(" " + flag + " ").getBytes();
        byte[] valueLen = String.valueOf(value.length).getBytes();
        byte[] keyBytes = key.getBytes();
        int size = VALUE_SPACE.length
                + keyBytes.length
                + flagBytes.length
                + valueLen.length
                + RETURN.length
                + value.length
                + RETURN.length;
        ByteBuffer entryBuffer = ByteBuffer.allocate(size);
        entryBuffer.put(VALUE_SPACE);
        entryBuffer.put(keyBytes);
        entryBuffer.put(flagBytes);
        entryBuffer.put(valueLen);
        entryBuffer.put(RETURN);
        entryBuffer.put(value);
        entryBuffer.put(RETURN);
        this.bytes = entryBuffer.array();
        this.flag = flag;
    }

    public MemcacheEntry() {
    }

    public void readData(DataInput in) throws IOException {
        int size = in.readInt();
        bytes = new byte[size];
        in.readFully(bytes);
        flag = in.readInt();
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeInt(bytes.length);
        out.write(bytes);
        out.writeInt(flag);
    }

    public ByteBuffer toNewBuffer() {
        return ByteBuffer.wrap(bytes);
    }

    public int getFlag() {
        return flag;
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public String toString() {
        return "MemcacheEntry{" +
                "bytes=" + new String(bytes) +
                ", flag=" + flag +
                '}';
    }
}
