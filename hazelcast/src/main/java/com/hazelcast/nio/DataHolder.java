/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.nio;

import java.nio.ByteBuffer;

public final class DataHolder {
    final ByteBuffer buffer;
    final int size;
    int partitionHash;

    public DataHolder(int size) {
        this.size = size;
        this.buffer = ByteBuffer.allocate(size);
    }

    public DataHolder(Data data) {
        this.size = data.size();
        this.buffer = ByteBuffer.wrap(data.buffer);
        this.partitionHash = data.getPartitionHash();
    }

    public boolean shouldRead() {
        return (size > 0 && buffer != null && buffer.hasRemaining());
    }

    public void read(ByteBuffer src) {
        IOUtil.copyToHeapBuffer(src, buffer);
    }

    public void setPartitionHash(int partitionHash) {
        this.partitionHash = partitionHash;
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
        Data data = new Data(buffer.array());
        data.setPartitionHash(partitionHash);
        return data;
    }

    @Override
    public String toString() {
        return "DataHolder{" +
                "size=" + size +
                '}';
    }
}

