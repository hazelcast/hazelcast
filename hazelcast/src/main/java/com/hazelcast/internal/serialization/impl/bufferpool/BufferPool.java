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

package com.hazelcast.internal.serialization.impl.bufferpool;

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.Data;

/**
 * The BufferPool allows the pooling of the {@link BufferObjectDataInput} and {@link BufferObjectDataOutput} instances.
 *
 * The BufferPool is accessed using the {@link BufferPoolThreadLocal} So each thread gets its own instance and therefore
 * it doesn't need to be thread-safe.
 */
public interface BufferPool {

    /**
     * Takes an BufferObjectDataOutput from the pool.
     *
     * @return the taken BufferObjectDataOutput.
     */
    BufferObjectDataOutput takeOutputBuffer();

    /**
     * Returns a BufferObjectDataOutput back to the pool.
     *
     * The implementation is free to not return the instance to the pool but just close it.
     *
     * @param out the BufferObjectDataOutput.
     */
    void returnOutputBuffer(BufferObjectDataOutput out);

    /**
     * Takes an BufferObjectDataInput from the pool and initializes it with the given data.
     *
     * The reason that Data is passed as argument, is that for HazelcastEnterprise different
     * BufferObjectDataInput can be returned based on the type of Data.
     *
     * @param data
     * @return the taken BufferObjectDataInput
     */
    BufferObjectDataInput takeInputBuffer(Data data);

    /**
     * Returns a BufferObjectDataInput back to the pool.
     *
     * The implementation is free to not return the instance to the pool.
     * In that case this call does not keep a reference to `BufferObjectDataInput` so that it can be
     * garbage-collected.
     *
     * @param in the BufferObjectDataInput.
     */
    void returnInputBuffer(BufferObjectDataInput in);
}
