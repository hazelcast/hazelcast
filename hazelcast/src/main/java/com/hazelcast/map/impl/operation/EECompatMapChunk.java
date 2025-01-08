/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.EECompatMapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Dummy class that allows deserialization of EnterpriseMapChunk in the OS during restart from OS to EE
 */
public class EECompatMapChunk extends MapChunk {

    @Override
    protected void writeMetadata(ObjectDataOutput out) throws IOException {
        // This code should never get executed, the EECompatMapChunk is only created when
        // an EE member sends the MapChunk to an OS member
        throw new UnsupportedOperationException();
    }

    @Override
    protected void readMetadata(ObjectDataInput in) throws IOException {
        super.readMetadata(in);

        // During the deserialization on the OS member, the merkle tree feature should be off,
        // leading to serializing null int array
        // We read the null from the input and throw it away
        in.readIntArray();
    }

    @Override
    public int getFactoryId() {
        return EECompatMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return EECompatMapDataSerializerHook.ENTERPRISE_MAP_CHUNK;
    }
}
