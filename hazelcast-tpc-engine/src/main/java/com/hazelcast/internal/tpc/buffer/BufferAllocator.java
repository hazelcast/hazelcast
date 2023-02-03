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

package com.hazelcast.internal.tpc.buffer;

import java.nio.ByteBuffer;

public interface BufferAllocator<T extends Buffer> {
    int DEFAULT_BUFFER_SIZE = 4096;

    default T allocate() {
        return allocate(DEFAULT_BUFFER_SIZE);
    }

    T allocate(int minSize);

    void free(T buf);

    void free(ByteBuffer chunk);
}
