/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Map;

public final class PartitionResponse implements IdentifiedDataSerializable {

    private int[] partitions;
    private Object[] results;

    public PartitionResponse() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public PartitionResponse(int[] partitions, Object[] results) {
        this.partitions = partitions;
        this.results = results;
    }

    public void addResults(Map<Integer, Object> partitionResults) {
        if (results == null) {
            return;
        }
        for (int i = 0; i < results.length; i++) {
            partitionResults.put(partitions[i], results[i]);
        }
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SpiDataSerializerHook.PARTITION_RESPONSE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeIntArray(partitions);
        int resultLength = (results != null ? results.length : 0);
        out.writeInt(resultLength);
        if (resultLength > 0) {
            for (Object result : results) {
                out.writeObject(result);
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        partitions = in.readIntArray();
        int resultLength = in.readInt();
        if (resultLength > 0) {
            results = new Object[resultLength];
            for (int i = 0; i < resultLength; i++) {
                results[i] = in.readObject();
            }
        }
    }
}
