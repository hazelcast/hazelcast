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

package com.hazelcast.concurrent.idgen;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IdGenerator;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Generates blocks with the help of an {@link IAtomicLong}
 * For each block, id generation is simply a volatile increment
 */
public class IdGeneratorImpl implements IdGenerator {

    public static final int BLOCK_SIZE = 10000;

    private static final AtomicIntegerFieldUpdater<IdGeneratorImpl> RESIDUE = AtomicIntegerFieldUpdater
            .newUpdater(IdGeneratorImpl.class, "residue");
    private static final AtomicLongFieldUpdater<IdGeneratorImpl> LOCAL = AtomicLongFieldUpdater
            .newUpdater(IdGeneratorImpl.class, "local");

    private final IAtomicLong blockGenerator;

    private volatile int residue = BLOCK_SIZE;
    private volatile long local = -1L;

    public IdGeneratorImpl(IAtomicLong blockGenerator) {
        this.blockGenerator = blockGenerator;
    }

    @Override
    public boolean init(long id) {
        if (id < 0) {
            return false;
        }
        long step = (id / BLOCK_SIZE);

        synchronized (this) {
            boolean init = blockGenerator.compareAndSet(0, step + 1);
            if (init) {
                LOCAL.set(this, step);
                RESIDUE.set(this, (int) (id % BLOCK_SIZE) + 1);
            }
            return init;
        }
    }

    @Override
    public long newId() {
        long block = local;
        int value = RESIDUE.getAndIncrement(this);

        if (local != block) {
            return newId();
        }

        if (value < BLOCK_SIZE) {
            return block * BLOCK_SIZE + value;
        }

        synchronized (this) {
            value = residue;
            if (value >= BLOCK_SIZE) {
                LOCAL.set(this, blockGenerator.getAndIncrement());
                RESIDUE.set(this, 0);
            }
        }
        return newId();
    }

    @Override
    public String getPartitionKey() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public void destroy() {
        synchronized (this) {
            blockGenerator.destroy();
            LOCAL.set(this, -1);
            RESIDUE.set(this, BLOCK_SIZE);
        }
    }
}
