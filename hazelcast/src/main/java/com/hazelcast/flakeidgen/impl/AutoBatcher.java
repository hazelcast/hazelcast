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

package com.hazelcast.flakeidgen.impl;

import com.hazelcast.internal.util.Clock;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * A utility to serve IDs from IdBatch one by one, watching for validity.
 * It's a separate class due to testability.
 */
public class AutoBatcher {
    private final int batchSize;
    private final long validity;

    private volatile Block block = new Block(new IdBatch(0, 0, 0), 0);

    private final IdBatchSupplier batchIdSupplier;

    public AutoBatcher(int batchSize, long validity, IdBatchSupplier idGenerator) {
        this.batchSize = batchSize;
        this.validity = validity;
        this.batchIdSupplier = idGenerator;
    }

    /**
     * Return next ID from current batch or get new batch from supplier if
     * current batch is spent or expired.
     */
    public long newId() {
        for (; ; ) {
            Block block = this.block;
            long res = block.next();
            if (res != Long.MIN_VALUE) {
                return res;
            }

            synchronized (this) {
                if (block != this.block) {
                    // new block was assigned in the meantime
                    continue;
                }
                this.block = new Block(batchIdSupplier.newIdBatch(batchSize), validity);
            }
        }
    }

    private static final class Block {
        private static final AtomicIntegerFieldUpdater<Block> NUM_RETURNED = AtomicIntegerFieldUpdater
                .newUpdater(Block.class, "numReturned");

        private final IdBatch idBatch;
        private final long invalidSince;
        private volatile int numReturned;

        private Block(IdBatch idBatch, long validity) {
            this.idBatch = idBatch;
            this.invalidSince = validity > 0 ? Clock.currentTimeMillis() + validity : Long.MAX_VALUE;
        }

        /**
         * Returns next ID or Long.MIN_VALUE, if there is none.
         */
        long next() {
            if (invalidSince <= Clock.currentTimeMillis()) {
                return Long.MIN_VALUE;
            }
            int index;
            do {
                index = numReturned;
                if (index == idBatch.batchSize()) {
                    return Long.MIN_VALUE;
                }
            } while (!NUM_RETURNED.compareAndSet(this, index, index + 1));
            return idBatch.base() + index * idBatch.increment();
        }
    }

    public interface IdBatchSupplier {
        IdBatch newIdBatch(int batchSize);
    }
}

