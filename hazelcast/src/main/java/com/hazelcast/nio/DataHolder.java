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

package com.hazelcast.nio;

import java.nio.ByteBuffer;

public final class DataHolder {
    final ByteBuffer buffer ;
    final int size;

    public DataHolder(int size) {
        this.size = size;
        this.buffer = ByteBuffer.allocate(size);

    }

    public DataHolder(Data data) {
        this.size = data.size();
        this.buffer = ByteBuffer.wrap(data.buffer);
    }

    public boolean shouldRead() {
        return (size > 0 && buffer != null && buffer.hasRemaining());
    }

    public void read(ByteBuffer src) {
        IOUtil.copyToHeapBuffer(src, buffer);
    }

    public int size() {
        return size;
    }

    public void postRead() {
        if (size > 0) {
            buffer.flip();
        }
    }

    public Data toData() {
        return new Data(buffer.array());
    }
}

