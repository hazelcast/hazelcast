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

package com.hazelcast.internal.serialization.impl.bufferpool;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.Closeable;
import java.util.Queue;

import static com.hazelcast.nio.IOUtil.closeResource;

/**
 * Default {BufferPool} implementation.
 *
 * This class is designed to that a subclass can be made. This is done for the Enterprise version.
 */
public class BufferPoolImpl implements BufferPool {
    protected final SerializationService serializationService;

    BufferObjectDataOutput pooledOutput1;
    BufferObjectDataOutput pooledOutput2;

    BufferObjectDataInput pooledInput1;
    BufferObjectDataInput pooledInput2;

    public BufferPoolImpl(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public BufferObjectDataOutput takeOutputBuffer() {
        BufferObjectDataOutput out = pooledOutput1;
        if (out != null) {
            pooledOutput1 = null;
        } else {
            out = pooledOutput2;
            if (out != null) {
                pooledOutput2 = null;
            } else {
                out = serializationService.createObjectDataOutput();
            }
        }

        return out;
    }

    @Override
    public void returnOutputBuffer(BufferObjectDataOutput out) {
        if (out == null) {
            return;
        }

        out.clear();

        if (pooledOutput1 == null) {
            pooledOutput1 = out;
        } else if (pooledOutput2 == null) {
            pooledOutput2 = out;
        } else {
            closeResource(out);
        }
    }

    @Override
    public BufferObjectDataInput takeInputBuffer(Data data) {
        BufferObjectDataInput in = pooledInput1;
        if (in != null) {
            pooledInput1 = null;
        } else {
            in = pooledInput2;
            if (in != null) {
                pooledInput2 = null;
            } else {
                in = serializationService.createObjectDataInput((byte[]) null);
            }
        }

        in.init(data.toByteArray(), HeapData.DATA_OFFSET);
        return in;
    }

    @Override
    public void returnInputBuffer(BufferObjectDataInput in) {
        if (in == null) {
            return;
        }

        in.clear();

        if (pooledInput1 == null) {
            pooledInput1 = in;
        } else if (pooledOutput2 == null) {
            pooledInput2 = in;
        } else {
            closeResource(in);
        }
    }
}
