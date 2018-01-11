/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class PortableCachePartitionLostEvent implements Portable {

    private int partitionId;

    private String uuid;

    public PortableCachePartitionLostEvent() {
    }

    public PortableCachePartitionLostEvent(int partitionId, String uuid) {
        this.partitionId = partitionId;
        this.uuid = uuid;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getUuid() {
        return uuid;
    }

    @Override
    public int getFactoryId() {
        return SpiPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return SpiPortableHook.CACHE_PARTITION_LOST_EVENT;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("p", partitionId);
        writer.writeUTF("u", uuid);

    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        partitionId = reader.readInt("p");
        uuid = reader.readUTF("u");
    }
}
