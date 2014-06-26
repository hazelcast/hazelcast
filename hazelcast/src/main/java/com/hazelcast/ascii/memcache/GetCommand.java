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
import com.hazelcast.nio.IOUtil;

import java.nio.ByteBuffer;

public class GetCommand extends AbstractTextCommand {
    final String key;
    ByteBuffer value;
    ByteBuffer lastOne;

    public GetCommand(TextCommandConstants.TextCommandType type, String key) {
        super(type);
        this.key = key;
    }

    public GetCommand(String key) {
        this(TextCommandConstants.TextCommandType.GET, key);
    }

    public String getKey() {
        return key;
    }

    public boolean readFrom(ByteBuffer cb) {
        return true;
    }

    public void setValue(MemcacheEntry entry, boolean singleGet) {
        if (entry != null) {
            value = entry.toNewBuffer();
        }
        lastOne = (singleGet) ? ByteBuffer.wrap(TextCommandConstants.END) : null;
    }

    public boolean writeTo(ByteBuffer bb) {
        if (value != null) {
            IOUtil.copyToHeapBuffer(value, bb);
        }
        if (lastOne != null) {
            IOUtil.copyToHeapBuffer(lastOne, bb);
        }
        return !((value != null && value.hasRemaining())
                || (lastOne != null && lastOne.hasRemaining()));
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
