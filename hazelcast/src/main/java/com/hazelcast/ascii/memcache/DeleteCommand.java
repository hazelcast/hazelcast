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

import java.nio.ByteBuffer;

public class DeleteCommand extends AbstractTextCommand {
    ByteBuffer response;
    private final String key;
    private final int expiration;
    private final boolean noreply;

    public DeleteCommand(String key, int expiration, boolean noreply) {
        super(TextCommandType.DELETE);
        this.key = key;
        this.expiration = expiration;
        this.noreply = noreply;
    }

    public boolean readFrom(ByteBuffer cb) {
        return true;
    }

    public void setResponse(byte[] value) {
        this.response = ByteBuffer.wrap(value);
    }

    public boolean writeTo(ByteBuffer bb) {
        if (response == null) {
            response = ByteBuffer.wrap(STORED);
        }
        while (bb.hasRemaining() && response.hasRemaining()) {
            bb.put(response.get());
        }
        return !response.hasRemaining();
    }

    public boolean shouldReply() {
        return !noreply;
    }

    public int getExpiration() {
        return expiration;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "DeleteCommand [" + type + "]{"
                + "key='" + key + '\''
                + ", expiration=" + expiration
                + ", noreply=" + noreply + '}'
                + super.toString();
    }
}
