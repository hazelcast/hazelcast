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

import com.hazelcast.ascii.AbstractTextCommand;
import com.hazelcast.ascii.TextCommandConstants;

import java.nio.ByteBuffer;

/**
 * User: sancar
 * Date: 3/8/13
 * Time: 3:33 PM
 */
public class IncrementCommand extends AbstractTextCommand {

    String key;
    int value;
    boolean noreply;
    ByteBuffer response;

    public IncrementCommand(TextCommandConstants.TextCommandType type, String key, int value, boolean noReply) {
        super(type);
        this.key = key;
        this.value = value;
        this.noreply = noReply;
    }

    public boolean writeTo(ByteBuffer destination) {
        while (destination.hasRemaining() && response.hasRemaining()) {
            destination.put(response.get());
        }
        return !response.hasRemaining();
    }

    public boolean readFrom(ByteBuffer source) {
        return true;
    }

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
