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

package com.hazelcast.ascii.memcache;

import com.hazelcast.ascii.TextCommandConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.hazelcast.util.StringUtil.bytesToString;
import static com.hazelcast.util.StringUtil.stringToBytes;

@edu.umd.cs.findbugs.annotations.SuppressWarnings("EI_EXPOSE_REP")
public class MemcacheEntry implements DataSerializable {
    private byte[] bytes;
    private byte[] value;
    private int flag;

    public MemcacheEntry(String key, byte[] value, int flag) {
        byte[] flagBytes = stringToBytes(" " + flag + " ");
        byte[] valueLen = stringToBytes(String.valueOf(value.length));
        byte[] keyBytes = stringToBytes(key);
        this.value = value.clone();
        int size = TextCommandConstants.VALUE_SPACE.length
                + keyBytes.length
                + flagBytes.length
                + valueLen.length
                + TextCommandConstants.RETURN.length
                + value.length
                + TextCommandConstants.RETURN.length;
        ByteBuffer entryBuffer = ByteBuffer.allocate(size);
        entryBuffer.put(TextCommandConstants.VALUE_SPACE);
        entryBuffer.put(keyBytes);
        entryBuffer.put(flagBytes);
        entryBuffer.put(valueLen);
        entryBuffer.put(TextCommandConstants.RETURN);
        entryBuffer.put(value);
        entryBuffer.put(TextCommandConstants.RETURN);
        this.bytes = entryBuffer.array();
        this.flag = flag;
    }

    public MemcacheEntry() {
    }

    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        bytes = new byte[size];
        in.readFully(bytes);
        size = in.readInt();
        value = new byte[size];
        in.readFully(value);
        flag = in.readInt();
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(bytes.length);
        out.write(bytes);
        out.writeInt(value.length);
        out.write(value);
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

    public byte[] getValue() {
        return value;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MemcacheEntry that = (MemcacheEntry) o;

        if (flag != that.flag) {
            return false;
        }
        if (!Arrays.equals(bytes, that.bytes)) {
            return false;
        }
        if (!Arrays.equals(value, that.value)) {
            return false;
        }

        return true;
    }

    public int hashCode() {
        int result = bytes != null ? Arrays.hashCode(bytes) : 0;
        result = 31 * result + (value != null ? Arrays.hashCode(value) : 0);
        result = 31 * result + flag;
        return result;
    }

    public String toString() {
        return "MemcacheEntry{"
                + "bytes="
                + bytesToString(bytes)
                + ", flag="
                + flag
                + '}';
    }
}
