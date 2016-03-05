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

package com.hazelcast.jet.memory.impl.operations;

import com.hazelcast.jet.memory.impl.util.Util;
import com.hazelcast.jet.memory.spi.operations.ContainersPull;
import com.hazelcast.jet.memory.spi.operations.ContainerFactory;

public class DefaultContainersPull<T> implements ContainersPull<T> {
    private T[] pool;
    private final int poolSize;

    @SuppressWarnings("unchecked")
    public DefaultContainersPull(ContainerFactory<T> containerFactory,
                                 int poolSize) {
        assert Util.isPositivePowerOfTwo(poolSize);

        this.poolSize = poolSize;
        this.pool = (T[]) new Object[poolSize];
        for (int i = 0; i < poolSize; i++) {
            this.pool[i] = containerFactory.createContainer();
        }
    }

    @Override
    public int size() {
        return poolSize;
    }

    @Override
    public T get(int position) {
        return pool[position];
    }
}
