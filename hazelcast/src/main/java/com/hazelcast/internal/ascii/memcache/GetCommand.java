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

import java.nio.ByteBuffer;

import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.GET;
import static com.hazelcast.internal.nio.IOUtil.copyToHeapBuffer;

public class GetCommand extends AbstractTextCommand {

    protected final String key;
    private ByteBuffer value;
    private ByteBuffer endMarker;

    public GetCommand(TextCommandConstants.TextCommandType type, String key) {
        super(type);
        this.key = key;
    }

    public GetCommand(String key) {
        this(GET, key);
    }

    public String getKey() {
        return key;
    }

    @Override
    public boolean readFrom(ByteBuffer src) {
        return true;
    }

    public void setValue(MemcacheEntry entry) {
        if (entry != null) {
            value = entry.toNewBuffer();
        }
        endMarker = ByteBuffer.wrap(TextCommandConstants.END);
    }

    @Override
    public boolean writeTo(ByteBuffer dst) {
        if (value != null) {
            copyToHeapBuffer(value, dst);
        }
        copyToHeapBuffer(endMarker, dst);
        return !((value != null && value.hasRemaining())
                || endMarker.hasRemaining());
    }

    @Override
    public String toString() {
        return "GetCommand{"
                + "key='"
                + key
                + ", value="
                + value
                + '\''
                + "} "
                + super.toString();
    }
}
