/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import java.nio.ByteBuffer;

public class IncrementCommand extends AbstractTextCommand {

    private String key;
    private int value;
    private boolean noreply;
    private ByteBuffer response;

    public IncrementCommand(TextCommandConstants.TextCommandType type, String key, int value, boolean noReply) {
        super(type);
        this.key = key;
        this.value = value;
        this.noreply = noReply;
    }

    @Override
    public boolean writeTo(ByteBuffer dst) {
        while (dst.hasRemaining() && response.hasRemaining()) {
            dst.put(response.get());
        }
        return !response.hasRemaining();
    }

    @Override
    public boolean readFrom(ByteBuffer src) {
        return true;
    }

    @Override
    public boolean shouldReply() {
        return !noreply;
    }

    public String getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }

    public void setResponse(byte[] value) {
        this.response = ByteBuffer.wrap(value);
    }
}
