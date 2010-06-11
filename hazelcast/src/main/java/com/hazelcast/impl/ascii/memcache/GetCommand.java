/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.impl.ascii.memcache;

import com.hazelcast.impl.ascii.AbstractTextCommand;
import com.hazelcast.nio.IOUtil;

import java.nio.ByteBuffer;

public class GetCommand extends AbstractTextCommand {
    final String key;
    ByteBuffer header;
    ByteBuffer value;
    ByteBuffer lastOne;

    public GetCommand(TextCommandType type, String key) {
        super(type);
        this.key = key;
    }

    public GetCommand(String key) {
        this(TextCommandType.GET, key);
    }

    public String getKey() {
        return key;
    }

    public boolean doRead(ByteBuffer cb) {
        return true;
    }

    public void setValue(byte[] value, boolean singleGet) {
        if (value == null) {
            if (singleGet) {
                lastOne = ByteBuffer.wrap(END);
            }
            return;
        }
        lastOne = ByteBuffer.wrap((singleGet) ? RETURN_END : RETURN);
        this.value = ByteBuffer.wrap(value);
        int valueLenInt = value.length;
        byte[] valueLen = String.valueOf(valueLenInt).getBytes();
        byte[] keyBytes = key.getBytes();
        int headerSize = VALUE_SPACE.length
                + keyBytes.length
                + FLAG_ZERO.length
                + valueLen.length
                + RETURN.length;
        header = ByteBuffer.allocate(headerSize);
        header.put(VALUE_SPACE);
        header.put(keyBytes);
        header.put(FLAG_ZERO);
        header.put(valueLen);
        header.put(RETURN);
        header.flip();
    }

    public boolean writeTo(ByteBuffer bb) {
        IOUtil.copyToHeapBuffer(header, bb);
        IOUtil.copyToHeapBuffer(value, bb);
        IOUtil.copyToHeapBuffer(lastOne, bb);
        return !(header != null && header.hasRemaining() || (value != null && value.hasRemaining()) || (lastOne != null && lastOne.hasRemaining()));
    }

    @Override
    public String toString() {
        return "GetCommand{" +
                "key='" + key + '\'' +
                '}' + super.toString();
    }
}