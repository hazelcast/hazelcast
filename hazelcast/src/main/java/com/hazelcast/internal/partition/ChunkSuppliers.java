/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition;

import com.hazelcast.internal.util.CollectionUtil;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nullable;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.Preconditions.checkNoNullInside;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.internal.util.Preconditions.isNotNull;

/**
 * Helper methods to create various {@link ChunkSupplier} objects
 */
public final class ChunkSuppliers {

    private ChunkSuppliers() {
    }

    /**
     * Intended use cases:
     * <ul>
     * <li>When migration data fits in one single chunk</li>
     * <li>When a service doesn't support data migration in chunks</li>
     * </ul>
     */
    public static ChunkSupplier newSingleChunkSupplier(Supplier<Operation> operationSupplier) {
        isNotNull(operationSupplier, "operationSupplier");

        return new SingleChunkSupplier(operationSupplier);
    }

    private static final class SingleChunkSupplier implements ChunkSupplier {

        private final Supplier<Operation> operationSupplier;

        private boolean hasMoreChunks = true;

        private SingleChunkSupplier(Supplier<Operation> operationSupplier) {
            this.operationSupplier = operationSupplier;
        }

        @Nullable
        @Override
        public Operation next() {
            if (!hasMoreChunks) {
                throw new NoSuchElementException();
            }
            Operation operation = operationSupplier.get();
            hasMoreChunks = false;
            return operation;
        }

        @Override
        public boolean hasNext() {
            return hasMoreChunks;
        }

        @Override
        public String toString() {
            return "SingleChunkSupplier{"
                    + "operationSupplier=" + operationSupplier
                    + ", hasMoreChunks=" + hasMoreChunks
                    + '}';
        }
    }

    /**
     * @param chain chain of {@link ChunkSupplier}
     * @return wrapper {@link ChunkSupplier}
     */
    public static ChunkSupplier newChainedChunkSupplier(List<ChunkSupplier> chain) {
        isNotNull(chain, "chain");
        checkTrue(CollectionUtil.isNotEmpty(chain), "chain cannot be an empty list");
        checkNoNullInside(chain, "chain cannot have null value inside");

        return new ChainedChunkSupplier(chain);
    }

    private static final class ChainedChunkSupplier implements ChunkSupplier {

        private final int length;
        private final List<ChunkSupplier> chain;

        private ChainedChunkSupplier(List<ChunkSupplier> chain) {
            this.chain = chain;
            this.length = chain.size();
        }

        @Override
        public void inject(BooleanSupplier isEndOfChunk) {
            for (ChunkSupplier chunkSupplier : chain) {
                chunkSupplier.inject(isEndOfChunk);
            }
        }

        @Override
        public boolean hasNext() {
            for (int i = 0; i < length; i++) {
                ChunkSupplier chunkSupplier = chain.get(i);
                if (chunkSupplier.hasNext()) {
                    return true;
                }
            }

            return false;
        }

        @Nullable
        @Override
        public Operation next() {
            for (int i = 0; i < length; i++) {
                ChunkSupplier chunkSupplier = chain.get(i);
                if (chunkSupplier.hasNext()) {
                    return chunkSupplier.next();
                }
            }

            throw new NoSuchElementException();
        }

        @Override
        public String toString() {
            return "ChunkSupplierChain{"
                    + "length=" + length
                    + ", chain=" + chain
                    + '}';
        }
    }
}
