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

package com.hazelcast.map.impl.event;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.BinaryInterface;

import java.io.IOException;

/**
 * Contains the data related to a map partition event
 */
@BinaryInterface
public class MapPartitionEventData extends AbstractEventData {

    private int partitionId;

    public MapPartitionEventData() {
    }

    public MapPartitionEventData(String source, String mapName, Address caller, int partitionId) {
        super(source, mapName, caller, -1);
        this.partitionId = partitionId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(partitionId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        partitionId = in.readInt();
    }

    @Override
    public String toString() {
        return "MapPartitionEventData{"
                + super.toString()
                + ", partitionId=" + partitionId
                + '}';
    }
}
