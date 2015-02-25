/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
 * Time: 1:54 PM
 */
public class TouchCommand extends AbstractTextCommand {

    String key;
    int expiration;
    boolean noreply;
    ByteBuffer response;

    public TouchCommand(TextCommandConstants.TextCommandType type, String key, int expiration, boolean noReply) {
        super(type);
        this.key = key;
        this.expiration = expiration;
        this.noreply = noReply;
    }

    public boolean writeTo(ByteBuffer destination) {
        if (response == null) {
            response = ByteBuffer.wrap(TextCommandConstants.STORED);
        }
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

    public int getExpiration() {
        return expiration;
    }

    public void setResponse(byte[] value) {
        this.response = ByteBuffer.wrap(value);
    }
}
