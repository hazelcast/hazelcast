/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.ascii.memcache;

import com.hazelcast.internal.ascii.AbstractTextCommand;
import com.hazelcast.internal.ascii.TextCommandConstants;
import com.hazelcast.internal.nio.IOUtil;

import static com.hazelcast.internal.util.JVMUtil.upcast;

import java.nio.ByteBuffer;

public class SetCommand extends AbstractTextCommand {
    private ByteBuffer response;
    private final String key;
    private final int flag;
    private final int expiration;
    private final int valueLen;
    private final boolean noreply;
    private final ByteBuffer bbValue;

    public SetCommand(TextCommandConstants.TextCommandType type, String key, int flag,
                      int expiration, int valueLen, boolean noreply) {
        super(type);
        this.key = key;
        this.flag = flag;
        this.expiration = expiration;
        this.valueLen = valueLen;
        this.noreply = noreply;
        bbValue = ByteBuffer.allocate(valueLen);
    }

    @Override
    public boolean readFrom(ByteBuffer src) {
        copy(src);
        if (!bbValue.hasRemaining()) {
            while (src.hasRemaining()) {
                char c = (char) src.get();
                if (c == '\n') {
                    upcast(bbValue).flip();
                    return true;
                }
            }
        }
        return false;
    }

    void copy(ByteBuffer cb) {
        if (cb.isDirect()) {
            int n = Math.min(cb.remaining(), bbValue.remaining());
            if (n > 0) {
                cb.get(bbValue.array(), bbValue.position(), n);
                upcast(bbValue).position(bbValue.position() + n);
            }
        } else {
            IOUtil.copyToHeapBuffer(cb, bbValue);
        }
    }

    public void setResponse(byte[] value) {
        this.response = ByteBuffer.wrap(value);
    }

    @Override
    public boolean writeTo(ByteBuffer dst) {
        if (response == null) {
            response = ByteBuffer.wrap(TextCommandConstants.STORED);
        }
        while (dst.hasRemaining() && response.hasRemaining()) {
            dst.put(response.get());
        }
        return !response.hasRemaining();
    }

    @Override
    public boolean shouldReply() {
        return !noreply;
    }

    public int getExpiration() {
        return expiration;
    }

    public String getKey() {
        return key;
    }

    public byte[] getValue() {
        return bbValue.array();
    }

    public int getFlag() {
        return flag;
    }

    @Override
    public String toString() {
        return "SetCommand [" + type + "]{"
                + "key='"
                + key
                + '\''
                + ", flag="
                + flag
                + ", expiration="
                + expiration
                + ", valueLen="
                + valueLen
                + ", value="
                + bbValue
                + '}'
                + super.toString();
    }
}



