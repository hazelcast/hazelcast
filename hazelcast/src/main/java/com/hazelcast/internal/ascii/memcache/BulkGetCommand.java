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
import java.util.Collection;
import java.util.List;

import static com.hazelcast.internal.nio.IOUtil.copyToHeapBuffer;
import static com.hazelcast.internal.util.JVMUtil.upcast;

public class BulkGetCommand extends AbstractTextCommand {

    private final List<String> keys;
    private ByteBuffer byteBuffer;

    protected BulkGetCommand(List<String> keys) {
        super(TextCommandConstants.TextCommandType.BULK_GET);
        this.keys = keys;
    }

    public List<String> getKeys() {
        return keys;
    }

    @Override
    public boolean readFrom(ByteBuffer src) {
        return true;
    }

    @Override
    public boolean writeTo(ByteBuffer dst) {
        copyToHeapBuffer(byteBuffer, dst);
        return !byteBuffer.hasRemaining();
    }

    public void setResult(Collection<MemcacheEntry> result) {
        int size = TextCommandConstants.END.length;
        for (MemcacheEntry entry : result) {
            size += entry.getBytes().length;
        }
        byteBuffer = ByteBuffer.allocate(size);
        for (MemcacheEntry entry : result) {
            byte[] bytes = entry.getBytes();
            byteBuffer.put(bytes);
        }
        byteBuffer.put(TextCommandConstants.END);
        upcast(byteBuffer).flip();
    }
}
